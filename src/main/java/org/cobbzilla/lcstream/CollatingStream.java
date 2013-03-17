package org.cobbzilla.lcstream;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * (c) Copyright 2013 Jonathan Cobb.
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
public class CollatingStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(CollatingStream.class);

    private final OutputStream out;
    private final int numTokensInDate;

    private final Set<String> completedFeeders = new HashSet<String>();

    public CollatingStream(OutputStream out, String dateTimeFormat) {
        this.out = out;
        this.dateTimeFormatter = DateTimeFormat.forPattern(dateTimeFormat);
        this.numTokensInDate = dateTimeFormat.split(" ").length;
    }

    private final DateTimeFormatter dateTimeFormatter;
    public DateTimeFormatter getDateTimeFormatter() { return dateTimeFormatter; }

    private final Map<String, CollationFeederStream> feeders = new ConcurrentHashMap<String, CollationFeederStream>();
    public int getNumFeeders () { return feeders.keySet().size(); }

    public synchronized OutputStream addFeeder(String name) {
        final CollationFeederStream feeder = new CollationFeederStream(name, this);
        feeders.put(name, feeder);
        return feeder;
    }

    public synchronized void removeFeeder(String name) throws IOException {
        LOG.info("removeFeeder(" + name + ")");
        completedFeeders.add(name);
    }

    @Override public void write(byte[] bytes) throws IOException { out.write(bytes); }
    @Override public void write(byte[] bytes, int start, int length) throws IOException { out.write(bytes, start, length); }
    @Override public void write(int i) throws IOException { out.write(i); }

    @Override
    public void flush() throws IOException {
        flush_internal();
        super.flush();
    }

    private final Map<String, FeederLine> pendingLines = new ConcurrentHashMap<String, FeederLine>();
    private synchronized void flush_internal() throws IOException {
        SortedSet<FeederLine> lines = mergeLines();
        LOG.debug(numLinesWritten + " lines already written, flushing " + lines.size() + " pending lines: " + lines);
        for (FeederLine line : lines) {
            LOG.debug("flush: writeLine(" + line + ")");
            writeLine(line);
        }
        out.flush();
    }

    private SortedSet<FeederLine> mergeLines() {
        final TreeSet<FeederLine> lines = new TreeSet<FeederLine>(pendingLines.values());
//        LOG.info("mergeLines returning lines:\n"+StringUtils.join(lines, "\n"));
        return lines;
    }

    @Override
    public void close() throws IOException {
        flush_internal();
        super.close();
    }

    private final AtomicInteger fudgeFactor = new AtomicInteger(0);
    private final AtomicInteger addFailCount = new AtomicInteger(0);
    protected synchronized boolean addLine(String name, String line) throws IOException {

        final long lineTime = parseTimeFromLine(line);
        final FeederLine feederLine = new FeederLine(name, line, lineTime);

        if (pendingLines.size() >= feeders.keySet().size() - fudgeFactor.get()) {
            final Iterator<FeederLine> iterator = mergeLines().iterator();
            final FeederLine feederLineToWrite = iterator.next();
            writeLine(feederLineToWrite);
            pendingLines.remove(feederLineToWrite.name);
        }

        final String linePrefix = line.trim().substring(16, 35);
        LOG.debug("addLine(" + name + ", " + linePrefix + ")");

        FeederLine existingLine = pendingLines.get(name);
        if (existingLine != null) {
            LOG.debug("addLine(" + name + ", " + linePrefix + "): returning false (already have a line waiting) pending=" + pendingLines.size() + " (" + pendingLines.keySet() + "), feeders=" + feeders.size() + " (" + feeders.keySet() + ")");
            if (addFailCount.incrementAndGet() > feeders.size()*10) {
                LOG.warn("addLine - detected deadlock, incrementing fudge factor to let things complete");
                fudgeFactor.incrementAndGet();
            }
            return false;
        }

        pendingLines.put(name, feederLine);
        addFailCount.set(0);
        return true;
    }

    protected long parseTimeFromLine(String line) {
        final String dateTimeString = StringUtils.join(Arrays.asList(line.split(" ")).subList(0, numTokensInDate), " ");
        return dateTimeFormatter.parseDateTime(dateTimeString).getMillis();
    }

    private volatile long lastTimeWritten = 0;
    private volatile int numLinesWritten = 0;
    private void writeLine(FeederLine line) throws IOException {

        LOG.debug("writeLine(" + line.name + ", " + line.line.trim().substring(16, 35) + ")");

        final long lineTime = parseTimeFromLine(line.line);
        if (lineTime < lastTimeWritten) {
            LOG.error("writeLine: somehow we are writing something that should have already been written (timestamp is younger than most recent written)");
        }
        lastTimeWritten = lineTime;

        write(composeLine(line).getBytes());
        numLinesWritten++;

        if (completedFeeders.contains(line.name)) {
            feeders.remove(line.name);
        }

        synchronized (this) {
            this.notifyAll();
        }
    }

    private String composeLine(FeederLine line) {
        return new StringBuilder().append(line.name).append(" ").append(line.line).toString();
    }

    private class FeederLine implements Comparable<FeederLine> {

        String name; String line; long lineTime;

        public FeederLine(String name, String line, long lineTime) {
            this.name = name; this.line = line; this.lineTime = lineTime;
        }

        @Override
        public int compareTo(FeederLine feederLine) {
            long timeDiff = this.lineTime - feederLine.lineTime;
            if (timeDiff != 0) {
                return timeDiff < 0 ? -1 : 1;
            }
            return this.name.compareTo(feederLine.name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FeederLine)) return false;
            FeederLine that = (FeederLine) o;
            return name.equals(that.name);
        }

        @Override public int hashCode() { return name.hashCode(); }

        @Override public String toString() { return name+": "+line.trim(); }

    }
}
