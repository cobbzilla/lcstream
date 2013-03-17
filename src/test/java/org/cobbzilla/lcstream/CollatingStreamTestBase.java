package org.cobbzilla.lcstream;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * (c) Copyright 2013 Jonathan Cobb.
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class CollatingStreamTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(CollatingStreamTestBase.class);

    public static final String DATE_TIME_FORMAT = "YYYY-MM-dd HH:mm:ss,SSS";
    public DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(DATE_TIME_FORMAT);

    protected MockFeeder buildFeeder(int numLinesPerFeeder, CollationFeederStream feederStream) {
        return new MockFeeder(feederStream.getName(), feederStream, dateTimeFormatter, numLinesPerFeeder);
    }

    public void testFeeders (int numFeeders, int numLinesPerFeeder) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CollatingStream collatingStream = new CollatingStream(out, DATE_TIME_FORMAT);

        List<CollationFeederStream> feederStreams = new ArrayList<CollationFeederStream>();
        for (int i=0; i< numFeeders; i++) {
            feederStreams.add((CollationFeederStream) collatingStream.addFeeder("feeder-" + i));
        }

        List<MockFeeder> mockFeeders = new ArrayList<MockFeeder>();
        for (CollationFeederStream feederStream : feederStreams) {
            mockFeeders.add(buildFeeder(numLinesPerFeeder, feederStream));
        }

        List<Thread> feederThreads = new ArrayList<Thread>();
        for (MockFeeder feeder : mockFeeders) {
            Thread t = new Thread(feeder);
            feederThreads.add(t);
            t.start();
        }

        for (Thread t : feederThreads) {
            t.join();
        }

        collatingStream.flush();

        String aggregateOutput = out.toString();
        final String[] lines = aggregateOutput.split("\n");
        final String printFriendlyLines = "\n"+ StringUtils.join(lines, "\n");

        Map<Integer, Set<Integer>> missingLines = new HashMap<Integer, Set<Integer>>();
        for (int i=0; i<numFeeders; i++) {
            final HashSet<Integer> linesForFeeder = new HashSet<Integer>();
            missingLines.put(i, linesForFeeder);
            for (int j=0; j<numLinesPerFeeder; j++) {
                linesForFeeder.add(j);
            }
        }

        String lastLine = null;
        int lineCount = 0;
        long lastLineTime = 0;
        for (String line : lines) {
            final List<String> tokens = Arrays.asList(line.split(" "));
            final String dateTimeString = StringUtils.join(tokens.subList(1, 3), " ");
            final long lineTime;
            try {
                lineTime = dateTimeFormatter.parseDateTime(dateTimeString).getMillis();
            } catch (IllegalArgumentException e) {
                continue;
            }
            lineCount++;

            final String feederNumber = tokens.get(0).split("-")[1];
            final String lineNumber = tokens.get(4).substring(1);
            missingLines.get(Integer.parseInt(feederNumber)).remove(Integer.parseInt(lineNumber));

            if (lastLineTime != 0) {
                assertTrue("lines not in proper order (this("+line+")="+lineTime+", last("+lastLine+")="+lastLineTime+"), lines="+printFriendlyLines, lineTime >= lastLineTime);
                assertNotSame("duplicate lines are bad", lastLine, line);
            }

            lastLineTime = lineTime;
            lastLine = line;
        }

        int totalMissing = 0;
        for (Integer feederNumber : missingLines.keySet()) {
            totalMissing += missingLines.get(feederNumber).size();
        }

        assertThat("incorrect number of lines, "+totalMissing+" missing lines were:"+missingLines+", all lines="+printFriendlyLines, lineCount, is(numFeeders * numLinesPerFeeder));
    }

}
