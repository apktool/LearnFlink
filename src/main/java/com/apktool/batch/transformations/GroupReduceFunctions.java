package com.apktool.batch.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class GroupReduceFunctions {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
            "Who's there?",
            "I think I hear them. Stand, ho! Who's there?"
        )
            .flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {

                @Override
                public void flatMap(String s, Collector<Tuple2<Integer, String>> collector) throws Exception {
                    for (String word : s.split(" ")) {
                        collector.collect(new Tuple2<>(1, word));
                    }
                }
            })
            .groupBy(0)
            .reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
                @Override
                public void reduce(Iterable<Tuple2<Integer, String>> iterable, Collector<Tuple2<Integer, String>> collector) throws Exception {

                    Set<String> uniqStrings = new HashSet<String>();
                    Integer key = null;

                    // add all strings of the group to the set
                    for (Tuple2<Integer, String> t : iterable) {
                        key = t.f0;
                        uniqStrings.add(t.f1);
                    }

                    // emit all unique strings.
                    for (String s : uniqStrings) {
                        collector.collect(new Tuple2<Integer, String>(key, s));
                    }
                }
            })
            .print();
    }
}
