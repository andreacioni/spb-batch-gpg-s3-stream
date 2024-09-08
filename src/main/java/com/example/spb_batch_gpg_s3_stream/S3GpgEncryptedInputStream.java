package com.example.spb_batch_gpg_s3_stream;

import com.amazonaws.services.s3.AmazonS3;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class S3GpgEncryptedInputStream extends InputStream {
    
    private static final Logger logger = LoggerFactory.getLogger(S3GpgEncryptedInputStream.class);

    private final AmazonS3 s3;
    private final String bucketName;
    private final String key;
    private int bufferSize = 4096;

    private final PipedInputStream pipeS3InputStream;
    private final PipedOutputStream pipeS3OutputStream;

    private final PipedInputStream pipeDecryptedInputStream;
    private final PipedOutputStream pipedDecryptedOutputStream;

    private boolean started = false;
    final CountDownLatch latch = new CountDownLatch(2);
    final ExecutorService executor = Executors.newFixedThreadPool(2);


    public S3GpgEncryptedInputStream(AmazonS3 s3, String bucketName, String key) throws IOException {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.key = key;

        pipeS3InputStream = new PipedInputStream();
        pipeS3OutputStream = new PipedOutputStream(pipeS3InputStream);

        pipeDecryptedInputStream = new PipedInputStream();
        pipedDecryptedOutputStream = new PipedOutputStream(pipeDecryptedInputStream);
    }

    @Override
    public int read() throws IOException {
        if(!started) {
            started = true;
            final var downloadTask = new Thread(() -> {
                try {
                    logger.info("Downloading");
                    s3StreamDownload(key);
                } catch (Throwable e ) {
                    logger.error("Error while downloading");
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();

                    try {
                        pipeS3OutputStream.close();
                    } catch (IOException e) {
                        logger.error("failed to close piped s3 stream", e);
                    }
                }

                logger.info("Download done");
            });

            final var decryptTask = new Thread(() -> {
                try {
                    logger.info("Decrypting stream");
                    pgpDecrypt();
                    logger.info("Decryption done");
                }
                catch(Throwable e) {
                    logger.error("Decryption failed");
                    throw new RuntimeException(e);
                }
                finally {
                    try {
                        pipeS3InputStream.close();
                    } catch (IOException e) {
                        logger.error("Error while closing decrypt streams", e);
                    }
                    latch.countDown();
                }
            });

            executor.execute(decryptTask);
            executor.execute(downloadTask);

            executor.shutdown();
        }

        if(latch.getCount() == 0) {
            return -1;
        }

        return pipeDecryptedInputStream.read();
    }

    @Override
    public void close() throws IOException {
        logger.debug("closing stream");
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.warn("latch await was interrupted", e);
        }

        pipedDecryptedOutputStream.close();
        pipeDecryptedInputStream.close();

        try {
            if(!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("await termination timed out after specified amount of time");
            }
        } catch (InterruptedException e) {
            logger.error("await termination interrupted", e);
        }
        started = false;
        logger.debug("stream closed");
        super.close();
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    private void s3StreamDownload(String key) throws IOException {
        final var obj = s3.getObject(bucketName, key);
        byte[] buff = new byte[bufferSize];
        int len;

        try (final var objectStream = obj.getObjectContent()) {
            while((len = objectStream.read(buff, 0, bufferSize)) >= 0) {
                pipeS3OutputStream.write(buff, 0, len);
            }
        }
    }

    private void pgpDecrypt() throws IOException, PGPException {
        final var privateKeyIn = new FileInputStream("src/main/resources/private.key");
        final var pgpSecretKeyRingCollection = new PGPSecretKeyRingCollection(
                PGPUtil.getDecoderStream(privateKeyIn), new JcaKeyFingerprintCalculator());

        // Removing armour and returning the underlying binary encrypted stream
        try (var encryptedIn = PGPUtil.getDecoderStream(pipeS3InputStream)) {
            JcaPGPObjectFactory pgpObjectFactory = new JcaPGPObjectFactory(encryptedIn);

            Object obj = pgpObjectFactory.nextObject();
            //The first object might be a marker packet
            PGPEncryptedDataList pgpEncryptedDataList = (obj instanceof PGPEncryptedDataList)
                    ? (PGPEncryptedDataList) obj : (PGPEncryptedDataList) pgpObjectFactory.nextObject();

            PGPPrivateKey pgpPrivateKey = null;
            PGPPublicKeyEncryptedData publicKeyEncryptedData = null;

            Iterator<PGPEncryptedData> encryptedDataItr = pgpEncryptedDataList.getEncryptedDataObjects();
            while (pgpPrivateKey == null && encryptedDataItr.hasNext()) {
                publicKeyEncryptedData = (PGPPublicKeyEncryptedData) encryptedDataItr.next();
                pgpPrivateKey = findSecretKey(pgpSecretKeyRingCollection, publicKeyEncryptedData.getKeyID());
            }

            if (Objects.isNull(publicKeyEncryptedData)) {
                throw new PGPException("Could not generate PGPPublicKeyEncryptedData object");
            }

            if (pgpPrivateKey == null) {
                throw new PGPException("Could Not Extract private key");
            }

            CommonUtils.decrypt(pipedDecryptedOutputStream, pgpPrivateKey, publicKeyEncryptedData);
        }

    }

    static private PGPPrivateKey findSecretKey(PGPSecretKeyRingCollection pgpSecretKeyRingCollection, long keyID) throws PGPException {
        PGPSecretKey pgpSecretKey = pgpSecretKeyRingCollection.getSecretKey(keyID);
        return pgpSecretKey == null ? null : pgpSecretKey.extractPrivateKey(new JcePBESecretKeyDecryptorBuilder()
                .setProvider(BouncyCastleProvider.PROVIDER_NAME).build("passw0rd".toCharArray()));
    }
}
