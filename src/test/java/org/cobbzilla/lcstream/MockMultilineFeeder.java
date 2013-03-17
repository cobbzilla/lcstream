package org.cobbzilla.lcstream;

import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.security.SecureRandom;

/**
 * (c) Copyright 2013 Jonathan Cobb.
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class MockMultilineFeeder extends MockFeeder {

    private static final Logger LOG = LoggerFactory.getLogger(MockMultilineFeeder.class);

    private SecureRandom random = new SecureRandom();

    public MockMultilineFeeder(String name, OutputStream out, DateTimeFormatter dateTimeFormat, int numLines) {
        super(name, out, dateTimeFormat, numLines);
    }

    @Override
    protected String nextBuffer(long instant, int lineNumber) {
        int numExtraLines = random.nextInt(5);
        StringBuilder builder = new StringBuilder(super.nextBuffer(instant, lineNumber));
        for (int i=0; i<numExtraLines; i++) {
            builder.append("extra line ").append(i).append(RandomStringUtils.randomAlphabetic(10)).append("\n");
        }
        return builder.toString();
    }
}
