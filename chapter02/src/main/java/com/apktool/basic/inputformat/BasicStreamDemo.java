package com.apktool.basic.inputformat;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;

public class BasicStreamDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamInputFormat inputFormat = new StreamInputFormat();

        TypeInformation<Tuple2<String, Integer>> info = TypeInformation.of(new TypeHint<>() {
        });

        // InputFormatSourceFunction<Tuple2<String, Integer>> function = new InputFormatSourceFunction<>(inputFormat, info);
        InputFormatSourceFunction<Tuple2<String, Integer>> function = new DtInputFormatSourceFunction<>(inputFormat, info);
        env.addSource(function, "demo", info)
            .map(t -> new Person(t.f0, t.f1))
            .filter(t -> t.age > 18)
            .map(t -> String.format("%s:%s", t.name, t.age))
            .print();

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
