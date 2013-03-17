package org.cobbzilla.lcstream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * (c) Copyright 2013 Jonathan Cobb.
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class MultilineCollatingStreamTest extends CollatingStreamTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MultilineCollatingStreamTest.class);

    @Test
    public void testBasicMultiline () throws Exception {
        testFeeders(1, 10);
    }

    @Test
    public void testManyFeedersMultiline () throws Exception {
        testFeeders(10, 100);
    }

    @Override
    protected MockFeeder buildFeeder(int numLinesPerFeeder, CollationFeederStream feederStream) {
        return new MockMultilineFeeder(feederStream.getName(), feederStream, dateTimeFormatter, numLinesPerFeeder);
    }
}
