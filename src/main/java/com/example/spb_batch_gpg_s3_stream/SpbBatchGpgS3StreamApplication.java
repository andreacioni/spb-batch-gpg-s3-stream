package com.example.spb_batch_gpg_s3_stream;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.security.Security;

@SpringBootApplication
public class SpbBatchGpgS3StreamApplication {

	public static void main(String[] args) {
		Security.addProvider(new BouncyCastleProvider());
		SpringApplication.run(SpbBatchGpgS3StreamApplication.class, args);
	}

}
