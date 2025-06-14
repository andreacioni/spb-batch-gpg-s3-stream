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
    ItemReader<String> downloadItemReader() throws IOException {
        final var inputStreamResource = new InputStreamResource(
                new S3InputStream(s3Client(),
                        "prova_stream_s3",
                        "prova.csv"));

        return new FlatFileItemReaderBuilder<String>()
                .name("downloadItemReader")
                .resource(inputStreamResource)
                .lineMapper(new PassThroughLineMapper(  ))
                .build();
    }

    @Bean
    ItemReader<String> flatFileItemReader() {
        return new FlatFileItemReaderBuilder<String>()
                .name("flatFileItemReader")
                .resource(new FileSystemResource("src/main/resources/prova.csv"))
                .lineMapper(new PassThroughLineMapper(  ))
                .build();
    }

    @Bean
    ItemWriter<String> flatFileItemWriter() {
        return new FlatFileItemWriterBuilder<String>()
                .name("flatFileItemWriter")
                .resource(new FileSystemResource("src/main/resources/prova.csv"))
                .lineAggregator(new PassThroughLineAggregator<>())
                .build();
    }

    //https://github.com/spring-projects/spring-batch/issues/3801
    @Bean(destroyMethod = "")
    ItemWriter<String> uploadItemWriter() throws IOException {
        final var outputStream = new S3OutputStream(s3Client(),
                "prova_stream_s3", "prova.csv");
        return new OutputStreamItemWriter(outputStream);
    }

    @Bean
    Job downloadJob(JobRepository jobRepository, Step downloadStep) {
        return new JobBuilder("downloadJob", jobRepository)
                .start(downloadStep)
                .build();
    }

    @Bean
    Job uploadJob(JobRepository jobRepository, Step uploadStep) {
        return new JobBuilder("uploadJob", jobRepository)
                .start(uploadStep)
                .build();
    }

    @Bean
    Step downloadStep(JobRepository jobRepository,
                           PlatformTransactionManager transactionManager) throws IOException {
        return new StepBuilder("downloadStep", jobRepository)
                .<String, String>chunk(10, transactionManager)
                .reader(downloadItemReader())
                .processor(new ItemProcessor())
                .writer(flatFileItemWriter())
                .build();
    }

    @Bean
    Step uploadStep(JobRepository jobRepository,
                     PlatformTransactionManager transactionManager) throws IOException {
        return new StepBuilder("uploadStep", jobRepository)
                .<String, String>chunk(10, transactionManager)
                .reader(flatFileItemReader())
                .processor(new ItemProcessor())
                .writer(uploadItemWriter())
                .build();
    }
}
