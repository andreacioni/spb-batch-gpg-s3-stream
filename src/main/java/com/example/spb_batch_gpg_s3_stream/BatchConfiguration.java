package com.example.spb_batch_gpg_s3_stream;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;

@Configuration
public class BatchConfiguration {

    @Bean
    AmazonS3 s3Client() {
        return  AmazonS3Client.builder()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("https://storage.googleapis.com", "aws-global"))
                .build();
    }

    @Bean
    ItemReader<String> decryptItemReader() throws IOException {
        final var inputStreamResource = new InputStreamResource(
                new S3GpgEncryptedInputStream(s3Client(),
                        "prova_stream_s3",
                        "prova.csv.gpg"));

        return new FlatFileItemReaderBuilder<String>()
                .name("decryptItemReader")
                .resource(inputStreamResource)
                .lineMapper(new PassThroughLineMapper(  ))
                .build();
    }

    @Bean
    ItemReader<String> encryptItemReader() {
        return new FlatFileItemReaderBuilder<String>()
                .name("encryptItemReader")
                .resource(new FileSystemResource("src/main/resources/prova.csv"))
                .lineMapper(new PassThroughLineMapper(  ))
                .build();
    }

    @Bean
    ItemWriter<String> decryptItemWriter() {
        return new FlatFileItemWriterBuilder<String>()
                .name("decryptItemWriter")
                .resource(new FileSystemResource("src/main/resources/prova.csv"))
                .lineAggregator(new PassThroughLineAggregator<>())
                .build();
    }

    @Bean
    ItemWriter<String> encryptItemWriter() throws IOException {
        final var outputStream = new S3GpgEncryptedOutputStream(s3Client(),
                "prova_stream_s3", "prova.csv.gpg");
        return new OutputStreamItemWriter(outputStream);
    }

    @Bean
    Job decryptJob(JobRepository jobRepository, Step decryptStep) {
        return new JobBuilder("decryptJob", jobRepository)
                .start(decryptStep)
                .build();
    }

    @Bean
    Job encryptJob(JobRepository jobRepository, Step encryptStep) {
        return new JobBuilder("encryptJob", jobRepository)
                .start(encryptStep)
                .build();
    }

    @Bean
    Step decryptStep(JobRepository jobRepository,
                           PlatformTransactionManager transactionManager) throws IOException {
        return new StepBuilder("decryptStep", jobRepository)
                .<String, String>chunk(10, transactionManager)
                .reader(decryptItemReader())
                .processor(new ItemProcessor())
                .writer(decryptItemWriter())
                .build();
    }

    @Bean
    Step encryptStep(JobRepository jobRepository,
                     PlatformTransactionManager transactionManager) throws IOException {
        return new StepBuilder("encryptStep", jobRepository)
                .<String, String>chunk(10, transactionManager)
                .reader(encryptItemReader())
                .processor(new ItemProcessor())
                .writer(encryptItemWriter())
                .build();
    }
}
