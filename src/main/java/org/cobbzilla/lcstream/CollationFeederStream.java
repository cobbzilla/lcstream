package org.cobbzilla.lcstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * (c) Copyright 2013 Jonathan Cobb.
 * This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html
 */
class CollationFeederStream extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(CollationFeederStream.class);

    private final String name;
    public String getName() { return name; }

    private final CollatingStream collatingStream;

    private final StringBuilder activeLine = new StringBuilder(1024);
    private final StringBuilder pendingLines = new StringBuilder(1024);

    public CollationFeederStream(String name, CollatingStream collatingStream) {
        this.name = name;
        this.collatingStream = collatingStream;
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(byte[] bytes, int start, int length) throws IOException {
        activeLine.append(new String(bytes, start, length));
        int newLinePos = 0;
        while (newLinePos != -1) {
            newLinePos = activeLine.indexOf("\n");
            if (newLinePos != -1) {
                addLine(activeLine.substring(0, newLinePos + 1));
                activeLine.delete(0, newLinePos + 1);
            }
        }
    }

    @Override
    public void write(int i) throws IOException {
        byte[] buf = new byte[1];
        buf[0] = (byte) i;
        write(buf);
    }

    private void addLine(String line) throws IOException {
        if (pendingLines.length() > 0) {
            if (isStartOfNewLogLine(line)) {
                sendLineToCollatingStream(pendingLines.toString());
                pendingLines.setLength(0);
            }
        }
        pendingLines.append(line);
    }

    private void sendLineToCollatingStream(String lines) throws IOException {
        while (!collatingStream.addLine(name, lines)) {
            synchronized (collatingStream) {
                try {
                    collatingStream.wait(collatingStream.getNumFeeders()*25);
                } catch (InterruptedException e) {
                    throw new IllegalArgumentException("interrupted", e);
                }
            }
        }
    }

    private boolean isStartOfNewLogLine(String line) {
        try {
            collatingStream.parseTimeFromLine(line);
        } catch (IllegalArgumentException e) {
            return false;
        }
        return true;
    }

    @Override
    public void flush() throws IOException {
        flush_internal();
        super.flush();
    }

    @Override
    public void close() throws IOException {
        flush_internal();
        collatingStream.removeFeeder(name);
        super.close();
    }

    private void flush_internal() throws IOException {
        if (pendingLines.length() > 0) {
            sendLineToCollatingStream(pendingLines.toString());
        }
    }
}
