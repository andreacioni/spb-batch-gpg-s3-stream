package com.example.spb_batch_gpg_s3_stream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.services.s3.internal.Constants.MB;

public class S3MultipartUpload {
    private static final Logger logger = LoggerFactory.getLogger(S3MultipartUpload.class);

    private final String destBucketName;
    private final String filename;

    private final String contentType;

    private final int uploadPartSize;
    
    private final int awaitTerminationSeconds;

    private final ThreadPoolExecutor executorService;
    private final AmazonS3 s3Client;

    private String uploadId;

    // uploadPartId should be between 1 to 10000 inclusively
    private final AtomicInteger uploadPartId = new AtomicInteger(0);

    private final List<Future<PartETag>> futuresPartETags = new ArrayList<>();

    private S3MultipartUpload(String destBucketName, String filename, AmazonS3 s3Client, String contentType, int threadCount, int uploadPartSize, int awaitTerminationSeconds) {
        this.executorService = new BlockingExecutor(threadCount);
        this.destBucketName = destBucketName;
        this.filename = filename;
        this.s3Client = s3Client;
        this.contentType = contentType;
        this.uploadPartSize = uploadPartSize;
        this.awaitTerminationSeconds = awaitTerminationSeconds;
    }

    public void upload(InputStream inputStream) throws IOException {
        initializeUpload();

        int bytesRead, bytesAdded = 0;
        byte[] data = new byte[uploadPartSize];

        try (ByteArrayOutputStream bufferOutputStream = new ByteArrayOutputStream()) {
            while ((bytesRead = inputStream.read(data, 0, data.length)) != -1) {

                bufferOutputStream.write(data, 0, bytesRead);

                if (bytesAdded < uploadPartSize) {
                    // continue writing to same output stream unless it's size gets more than UPLOAD_PART_SIZE
                    bytesAdded += bytesRead;
                    continue;
                }
                uploadPartAsync(new ByteArrayInputStream(bufferOutputStream.toByteArray()));
                bufferOutputStream.reset(); // flush the bufferOutputStream
                bytesAdded = 0; // reset the bytes added to 0
            }

            // upload remaining part of output stream as final part
            // bufferOutputStream size can be less than 5 MB as it is the last part of upload
            uploadFinalPartAsync(new ByteArrayInputStream(bufferOutputStream.toByteArray()));
        } finally {
            this.shutdownAndAwaitTermination();
        }
    }

    /**
     * We need to call initialize upload method before calling any upload part.
     */
    private void initializeUpload() {

        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(destBucketName, filename);
        initRequest.setObjectMetadata(getObjectMetadata()); // if we want to set object metadata in S3 bucket
        initRequest.setTagging(getObjectTagging()); // if we want to set object tags in S3 bucket

        uploadId = s3Client.initiateMultipartUpload(initRequest).getUploadId();
    }

    private void uploadPartAsync(ByteArrayInputStream inputStream) {
        submitTaskForUploading(inputStream, false);
    }

    private void uploadFinalPartAsync(ByteArrayInputStream inputStream) {
        try {
            submitTaskForUploading(inputStream, true);

            // wait and get all PartETags from ExecutorService and submit it in CompleteMultipartUploadRequest
            List<PartETag> partETags = new ArrayList<>();
            for (Future<PartETag> partETagFuture : futuresPartETags) {
                partETags.add(partETagFuture.get());
            }

            // Complete the multipart upload
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(destBucketName, filename, uploadId, partETags);
            s3Client.completeMultipartUpload(completeRequest);

        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    /**
     * This method is used to shutdown executor service
     */
    private void shutdownAndAwaitTermination() {
        logger.debug("executor service await and shutdown");
        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(awaitTerminationSeconds, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            logger.debug("Interrupted while awaiting ThreadPoolExecutor to shutdown");
        }
        this.executorService.shutdownNow();
    }

    private void submitTaskForUploading(ByteArrayInputStream inputStream, boolean isFinalPart) {
        if (uploadId == null || uploadId.isEmpty()) {
            throw new IllegalStateException("Initial Multipart Upload Request has not been set.");
        }

        if (destBucketName == null || destBucketName.isEmpty()) {
            throw new IllegalStateException("Destination bucket has not been set.");
        }

        if (filename == null || filename.isEmpty()) {
            throw new IllegalStateException("Uploading file name has not been set.");
        }

        submitTaskToExecutorService(() -> {
            int eachPartId = uploadPartId.incrementAndGet();
            UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(destBucketName)
                    .withKey(filename)
                    .withUploadId(uploadId)
                    .withPartNumber(eachPartId) // partNumber should be between 1 and 10000 inclusively
                    .withPartSize(inputStream.available())
                    .withInputStream(inputStream);

            if (isFinalPart) {
                uploadRequest.withLastPart(true);
            }

            logger.debug(String.format("Submitting uploadPartId: %d of partSize: %d", eachPartId, inputStream.available()));

            UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);

            logger.debug(String.format("Successfully submitted uploadPartId: %d", eachPartId));
            return uploadResult.getPartETag();
        });
    }

    private void submitTaskToExecutorService(Callable<PartETag> callable) {
        // we are submitting each part in executor service and it does not matter which part gets upload first
        // because in each part we have assigned PartNumber from "uploadPartId.incrementAndGet()"
        // and S3 will accumulate file by using PartNumber order after CompleteMultipartUploadRequest
        Future<PartETag> partETagFuture = this.executorService.submit(callable);
        this.futuresPartETags.add(partETagFuture);
    }

    private ObjectTagging getObjectTagging() {
        // create tags list for uploading file
        return new ObjectTagging(new ArrayList<>());
    }

    private ObjectMetadata getObjectMetadata() {
        // create metadata for uploading file
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(contentType);
        return objectMetadata;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String bucketName;

        private String key;

        private AmazonS3 s3Client;

        private String contentType = "application/octet-stream";

        private int concurrentUpload = 1;

        private int partSize = 10 * MB;
        
        private int awaitTerminationSeconds = 2;

        public Builder() {
        }

        public S3MultipartUpload build() {
            if(s3Client == null) {
                throw new IllegalArgumentException("s3 client not supplied");
            }

            if(bucketName == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("bucket name is null/empty");
            }

            if(key == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("object key is null/empty");
            }

            return new S3MultipartUpload(
                    bucketName,
                    key,
                    s3Client,
                    contentType,
                    concurrentUpload,
                    partSize,
                    awaitTerminationSeconds);
        }

        public int getConcurrentUpload() {
            return concurrentUpload;
        }

        public Builder setConcurrentUpload(int concurrentUpload) {
            this.concurrentUpload = concurrentUpload;
            return this;
        }

        public Builder setS3Client(AmazonS3 s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public AmazonS3 getS3Client() {
            return s3Client;
        }

        public int getPartSize() {
            return partSize;
        }

        public Builder setPartSize(int partSize) {
            this.partSize = partSize;
            return this;
        }

        public Builder setKey(String key) {
            this.key = key;
            return this;
        }

        public String getKey() {
            return key;
        }

        public Builder setBucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public String getBucketName() {
            return bucketName;
        }

        public int getAwaitTerminationSeconds() {
            return awaitTerminationSeconds;
        }

        public void setAwaitTerminationSeconds(int awaitTerminationSeconds) {
            this.awaitTerminationSeconds = awaitTerminationSeconds;
        }

        public String getContentType() {
            return contentType;
        }

        public void setContentType(String contentType) {
            this.contentType = contentType;
        }
    }

    private static class BlockingExecutor extends ThreadPoolExecutor {
        private final Semaphore semaphore;

        public BlockingExecutor(final int concurrentTasksLimit) {
            super(concurrentTasksLimit, concurrentTasksLimit,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());

            semaphore = new Semaphore(concurrentTasksLimit);
        }

        @Override
        public void execute(final Runnable command) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                logger.error("failed to acquire semaphore", e);
                return;
            }

            final Runnable wrapped = () -> {
                try {
                    command.run();
                } finally {
                    semaphore.release();
                }
            };

            super.execute(wrapped);
        }
    }
}