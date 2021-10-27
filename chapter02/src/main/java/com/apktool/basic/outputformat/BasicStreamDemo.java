package com.apktool.basic.outputformat;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class BasicStreamDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamOutputFormat<Tuple2<String, Integer>> format = new StreamOutputFormat();
        SinkFunction<Tuple2<String, Integer>> function = new DtOutputFormatSinkFunction<>(format);

        env.fromElements(
            new Person("Fred", 35),
            new Person("Wilma", 32),
            new Person("Alias", 2)
        )
            .filter(t -> t.age > 18)
            .map(t -> new Tuple2<>(t.name, t.age))
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .addSink(function);

        env.execute();
    }


    private static class Person {
        protected Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        private String name;
        private Integer age;
    }
}
