package com.example.spb_batch_gpg_s3_stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;

import java.io.*;
import java.net.URI;
import java.net.URL;

public class OutputStreamItemWriter implements ItemWriter<String>, ItemStream {

    private final Logger logger = LoggerFactory.getLogger(OutputStreamItemWriter.class);
    private final OutputStream outputStream;

    public OutputStreamItemWriter(OutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(Chunk<? extends String> chunk) throws Exception {
        for (String item : chunk.getItems()) {
            outputStream.write(item.getBytes());
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            outputStream.close();
        } catch (IOException e) {
            logger.error("failed to close output stream", e);
            throw new ItemStreamException("failed to close output stream", e);
        }
        ItemStream.super.close();
    }
}
