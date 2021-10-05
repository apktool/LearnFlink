package com.apktool.datastream.sourcesink;

import org.apache.flink.api.common.functions.FilterFunction;

public class BasicFilter implements FilterFunction<Person> {
    @Override
    public boolean filter(Person person) throws Exception {
        return person.getAge() > 18;
    }
}
