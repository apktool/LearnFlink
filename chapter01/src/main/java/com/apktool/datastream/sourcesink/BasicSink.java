package com.apktool.datastream.sourcesink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class BasicSink implements SinkFunction<Person> {
    @Override
    public void invoke(Person value, Context context) throws Exception {
        System.out.printf("%s:%d\n", value.getName(), value.getAge());
    }
}
