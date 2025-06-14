package com.example.spb_batch_gpg_s3_stream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.MultipartUpload;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.CompressionAlgorithmTags;
import org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.jcajce.JcaPGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyKeyEncryptionMethodGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.SecureRandom;
import java.util.Iterator;
import java.util.concurrent.*;

public class S3OutputStream extends OutputStream {

    private static final Logger logger = LoggerFactory.getLogger(S3OutputStream.class);

    private final AmazonS3 s3;
    private final String bucketName;
    private final String key;
    private final PipedInputStream pipedInputStream = new PipedInputStream();
    private final PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream);
    private final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();

    private boolean uploading = false;
    private int concurrency = 1;
    private S3MultipartUpload multipartUpload;

    public S3OutputStream(AmazonS3 s3, String bucketName, String key) throws IOException {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.key = key;
        buildMultipartUpload();
    }
    public S3OutputStream(AmazonS3 s3, String bucketName, String key, int concurrency) throws IOException {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.key = key;
        this.concurrency = concurrency;
        buildMultipartUpload();
    }

    @Override
    public void write(int b) throws IOException {
        if(!uploading) {
            uploading = true;
            singleThreadExecutor.execute(() -> {
                try {
                    multipartUpload.upload(pipedInputStream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            singleThreadExecutor.shutdown();
        }

        pipedOutputStream.write(b);
    }

    @Override
    public void close() throws IOException {
        logger.debug("closing output stream");
        pipedOutputStream.close();
        pipedInputStream.close();
        logger.debug("output stream closed");
        super.close();
    }

    private void buildMultipartUpload() {
        multipartUpload = S3MultipartUpload.builder()
                .setS3Client(s3)
                .setBucketName(bucketName)
                .setConcurrentUpload(concurrency)
                .setKey(key)
                .build();
    }
}
