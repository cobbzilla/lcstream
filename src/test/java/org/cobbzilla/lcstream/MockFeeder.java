package org.cobbzilla.lcstream;

import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.security.SecureRandom;

/**
 * (c) Copyright 2013 Jonathan Cobb.
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class MockFeeder implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(MockFeeder.class);

    private static final SecureRandom random = new SecureRandom();

    private final String name;
    private final OutputStream out;
    private final DateTimeFormatter dateTimeFormatter;
    private final int numLines;

    public MockFeeder(String name, OutputStream out, DateTimeFormatter dateTimeFormat, int numLines) {
        this.name = name;
        this.out = out;
        this.dateTimeFormatter = dateTimeFormat;
        this.numLines = numLines;
    }

    @Override
    public void run() {
        try {
            run_internal();
        } catch (Exception e) {
            throw new IllegalStateException("error writing: "+e, e);
        } finally {
            LOG.info(name+" - exiting after having written "+numLines+" lines");
        }
    }

    private void run_internal() throws Exception {

        long instant = System.currentTimeMillis();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(numLines*100);

        // build the entire buffer first
        for (int i=0; i<numLines; i++) {
            instant += random.nextInt(50);
            buffer.write(nextBuffer(instant, i).getBytes());
        }

        // send the buffer to the collator in 1k chunks
        byte[] rawLog = buffer.toByteArray();
        int startIndex = 0;
        int blockSize = 1024;
        while (startIndex < rawLog.length) {
            if (startIndex+blockSize > rawLog.length) {
                blockSize = (rawLog.length - startIndex);
            }
            out.write(rawLog, startIndex, blockSize);
            startIndex += blockSize;
            Thread.sleep(random.nextInt(20));
        }
        out.close();
    }

    protected String nextBuffer(long instant, int lineNumber) {
        StringBuilder line = new StringBuilder();
        return line.append(new DateTime(instant).toString(dateTimeFormatter)).append(" line #").append(lineNumber).append(" ").append(RandomStringUtils.randomAlphanumeric(5)).append("\n").toString();
    }
}
