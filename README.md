lcstream
========

lcstream = Log Collating Stream. Read from and collate multiple log files into a single chronologically ordered stream.

(c) Copyright 2013 Jonathan Cobb.
This code is available under the Apache License, version 2: http://www.apache.org/licenses/LICENSE-2.0.html

### Usage

lcstream assumes that the timestamps appear at the beginning of each log line.
lcstream does not yet support logs where the timestamps appear in the middle of the line.

    OutputStream out = .... // this is where the CollatingStream will write collated output
    String DATE_TIME_FORMAT = "YYYY-MM-dd HH:mm:ss,SSS"; // the date-time format. uses joda-time.
    CollatingStream collatingStream = new CollatingStream(out, DATE_TIME_FORMAT);

    OutputStream out1 = collatingStream.addFeeder("feeder1");
    OutputStream out2 = collatingStream.addFeeder("feeder2");

    // ... spawn threads to write lots of log data concurrently to out1 and out2 ...

    // then, once your threads are done writing to out1 and out2, you must call close (or flush)
    // to ensure the last few lines get sent to the CollatingStream
    out1.close();
    out2.close();

    collatingStream.close(); // or flush, sends any remaining log lines to out

