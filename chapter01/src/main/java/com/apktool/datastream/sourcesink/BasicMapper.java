package com.apktool.datastream.sourcesink;

import org.apache.flink.api.common.functions.MapFunction;

public class BasicMapper implements MapFunction<String, Person> {
    @Override
    public Person map(String o) throws Exception {
        String[] tmp = o.split(",");
        return new Person(tmp[0], Integer.valueOf(tmp[1]));
    }
}
