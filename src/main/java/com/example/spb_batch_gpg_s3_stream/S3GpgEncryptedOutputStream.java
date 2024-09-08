package com.example.spb_batch_gpg_s3_stream;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class S3GpgEncryptedOutputStream extends OutputStream {

    private static final Logger logger = LoggerFactory.getLogger(S3GpgEncryptedOutputStream.class);

    private final AmazonS3 s3;
    private final String bucketName;
    private final String key;
    private int bufferSize = 4096;
    private boolean armored = false;
    private int concurrency =1;

    private final PipedInputStream pipeEncryptInputStream;
    private final PipedOutputStream pipeEncryptOutputStream;

    private final PipedOutputStream pipePlainTextOutputStream;
    private final PipedInputStream pipePlainTextInputStream;

    private boolean started = false;
    final CountDownLatch latch = new CountDownLatch(2);
    final ExecutorService executor = Executors.newFixedThreadPool(2);


    public S3GpgEncryptedOutputStream(AmazonS3 s3, String bucketName, String key) throws IOException {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.key = key;

        pipeEncryptInputStream = new PipedInputStream();
        pipeEncryptOutputStream = new PipedOutputStream(pipeEncryptInputStream);

        pipePlainTextInputStream = new PipedInputStream();
        pipePlainTextOutputStream = new PipedOutputStream(pipePlainTextInputStream);


    }

    @Override
    public void write(int b) throws IOException {
        if(!started) {
            started = true;
            final var encryptTask = new Thread(() -> {
                try {
                    logger.debug("Encrypting");
                    pgpEncrypt();
                } catch (Exception e) {
                    logger.error("Error while encrypting", e);
                    throw new RuntimeException(e);
                } finally {
                    try {
                        pipeEncryptOutputStream.close();
                    } catch (Exception e) {
                        logger.error("Error while closing encryption streams", e);
                    }

                    latch.countDown();
                }

                logger.debug("Encrypting done");
            });

            final var uploadTask = new Thread(() -> {
                try {
                    logger.debug("Uploading to S3 bucket...");

                    uploadToS3();
                    logger.debug("Upload done");
                }
                catch(Exception e) {
                    logger.error("Upload failed", e);
                }
                finally {
                    try {
                        pipeEncryptInputStream.close();
                    } catch (Throwable e) {
                        logger.error("Error while closing upload streams",e);
                    }
                    latch.countDown();
                }
            });

            executor.execute(uploadTask);
            executor.execute(encryptTask);

            executor.shutdown();
        }

        pipePlainTextOutputStream.write(b);
    }

    @Override
    public void close() throws IOException {
        if(started) {
            pipePlainTextOutputStream.close();

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.warn("latch await was interrupted", e);
            }


            try {
                if(!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.warn("await termination timed out after specified amount of time");
                }
            } catch (InterruptedException e) {
                logger.error("await termination interrupted", e);
            }
            started = false;
        }

        super.close();
    }

    public int getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    public boolean isArmored() {
        return armored;
    }

    public void setArmored(boolean armored) {
        this.armored = armored;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    private void pgpEncrypt() throws PGPException, IOException {
        PGPCompressedDataGenerator compressedDataGenerator =
                new PGPCompressedDataGenerator(CompressionAlgorithmTags.ZLIB);
        PGPEncryptedDataGenerator pgpEncryptedDataGenerator = new PGPEncryptedDataGenerator(
                // This bit here configures the encrypted data generator
                new JcePGPDataEncryptorBuilder(SymmetricKeyAlgorithmTags.AES_256)
                        .setWithIntegrityPacket(true)
                        .setSecureRandom(new SecureRandom())
                        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        );
        // Adding public key
        pgpEncryptedDataGenerator.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(
                getPublicKey("src/main/resources/public.key")));

        OutputStream outputStream = pipeEncryptOutputStream;
        if(armored) {
            outputStream = new ArmoredOutputStream(outputStream);
        }
        OutputStream cipherOutStream = null;

        try {
            cipherOutStream = pgpEncryptedDataGenerator.open(outputStream, new byte[bufferSize]);
            CommonUtils.copyAsLiteralData(compressedDataGenerator.open(cipherOutStream), pipePlainTextInputStream,  bufferSize);
        } finally {
            // Closing all output streams in sequence
            if(compressedDataGenerator != null) {
                compressedDataGenerator.close();
            }

            if(cipherOutStream != null) {
                cipherOutStream.close();
            }

            if(outputStream != null) {
                outputStream.close();
            }

            pipePlainTextInputStream.close();
        }



    }

    private PGPPublicKey getPublicKey(String fileName) throws IOException, PGPException {
        InputStream in = new FileInputStream(fileName);
        in = org.bouncycastle.openpgp.PGPUtil.getDecoderStream(in);

        PGPPublicKeyRingCollection pgpPubRingCollection = new JcaPGPPublicKeyRingCollection(in);

        PGPPublicKey key = null;

        Iterator<PGPPublicKeyRing> rIt = pgpPubRingCollection.getKeyRings();

        while (key == null && rIt.hasNext()) {
            PGPPublicKeyRing kRing = rIt.next();
            Iterator<PGPPublicKey> kIt = kRing.getPublicKeys();
            while (key == null && kIt.hasNext()) {
                PGPPublicKey k = kIt.next();
                if (k.isEncryptionKey()) {
                    key = k;
                }
            }
        }

        if (key == null) {
            throw new IllegalArgumentException("Can't find encryption key in key ring.");
        }

        return key;
    }

    private void uploadToS3() throws IOException {
        final var multipartUpload = S3MultipartUpload.builder()
                .setS3Client(s3)
                .setBucketName(bucketName)
                .setConcurrentUpload(concurrency)
                .setKey(key)
                .build();

        multipartUpload.upload(pipeEncryptInputStream);
    }
}
