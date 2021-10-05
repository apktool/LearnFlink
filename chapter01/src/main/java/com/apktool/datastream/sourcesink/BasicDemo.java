package com.apktool.datastream.sourcesink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BasicDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        String[] persons = {"tmo,12", "bob,19", "alice,24"};

        env.addSource(new BasicSource(persons))
            .map(new BasicMapper())
            .filter(new BasicFilter())
            .addSink(new BasicSink());

        env.execute();
    }
}
