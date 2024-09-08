package com.example.spb_batch_gpg_s3_stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class ItemProcessor implements org.springframework.batch.item.ItemProcessor<String, String> {
    private static final Logger logger = LoggerFactory.getLogger(ItemProcessor.class);

    private final AtomicLong counter;

    public ItemProcessor() {
        this.counter = new AtomicLong(0);
    }

    @Override
    public String process(String item) throws Exception {
        logger.trace("({}) read line: {}", counter.addAndGet(1), item);
        return item;
    }
}
