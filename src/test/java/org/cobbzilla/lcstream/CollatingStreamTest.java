package org.cobbzilla.lcstream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * (c) Copyright 2013 Jonathan Cobb.
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class CollatingStreamTest extends CollatingStreamTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(CollatingStreamTest.class);

    @Test
    public void testOneFeederFewLines () throws Exception {
        testFeeders(1, 10);
    }

    @Test
    public void testOneFeederManyLines () throws Exception {
        testFeeders(1, 500);
    }

    @Test
    public void testTwoFeedersFewLines () throws Exception {
        testFeeders(2, 20);
    }

    @Test
    public void testTwoFeedersManyLines () throws Exception {
        testFeeders(2, 500);
    }

    @Test
    public void testManyFeedersFewLines () throws Exception {
        testFeeders(50, 10);
    }

    @Test
    public void testManyFeedersManyLines () throws Exception {
        testFeeders(20, 250);
    }

}
