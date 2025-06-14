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

public class S3InputStream extends InputStream {

    private static final Logger logger = LoggerFactory.getLogger(S3InputStream.class);

    private final AmazonS3 s3;
    private final String bucketName;
    private final String objectKey;

    InputStream inputStream = null;

    public S3InputStream(AmazonS3 s3, String bucketName, String objectKey) throws IOException {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.objectKey = objectKey;
    }

    @Override
    public int read() throws IOException {
        if(inputStream == null) {
            inputStream = getS3InputString();
        }

        return inputStream.read();
    }

    @Override
    public void close() throws IOException {
        logger.debug("closing stream");
        if(inputStream != null) {
            inputStream.close();
        }
        logger.debug("stream closed");
        super.close();
    }

    private InputStream getS3InputString() throws IOException {
        return s3.getObject(bucketName, objectKey).getObjectContent();
    }
}
