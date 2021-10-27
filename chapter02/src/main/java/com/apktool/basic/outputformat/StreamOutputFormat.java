package com.apktool.basic.outputformat;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

public class StreamOutputFormat<IN> extends RichOutputFormat<IN> {
    private PrintStream stream;
    private transient String completedPrefix;

    @Override
    public void configure(Configuration configuration) {
        stream = System.out;
        completedPrefix = "";
    }

    @Override
    public void open(int taskNumber, int numTasks) {
        this.completedPrefix = this.completedPrefix + (taskNumber + 1);
    }

    @Override
    public void writeRecord(IN record) {
        stream.println(String.format("%s: %s", completedPrefix, record));
    }

    @Override
    public void close() throws IOException {
        stream = null;
    }
}
