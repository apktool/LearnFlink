package com.apktool.batch.transformations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class SimpleFunctions {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements(
            "Who's there?",
            "I think I hear them. Stand, ho! Who's there?"
        );

        // Map
        text.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.split(" ").length;
            }
        })
            .print();

        // FlatMap
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
            .groupBy(0)
            .sum(1)
            .print();

        // MapPartition
        text.mapPartition(new MapPartitionFunction<String, Long>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<Long> out) {
                long c = 0;
                for (String s : values) {
                    c++;
                }
                out.collect(c);
            }
        })
            .print();

        // filter
        text.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.split(" ").length > 5;
            }
        })
            .print();

        // project
        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : s.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
            .<Tuple1<String>>project(1)
            .print();
    }
}
