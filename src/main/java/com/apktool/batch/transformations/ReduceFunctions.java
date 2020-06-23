package com.apktool.batch.transformations;

import com.apktool.common.KeyValue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

public class ReduceFunctions {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
            "Who's there?",
            "I think I hear them. Stand, ho! Who's there?"
        )
            .flatMap(new FlatMapFunction<String, KeyValue>() {
                @Override
                public void flatMap(String s, Collector<KeyValue> collector) throws Exception {
                    for (String word : s.split(" ")) {
                        collector.collect(new KeyValue(word, 1));
                    }
                }
            })
            .groupBy(new KeySelector<KeyValue, String>() {
                @Override
                public String getKey(KeyValue wc) throws Exception {
                    return wc.getKey();
                }
            })
            .reduce(new ReduceFunction<KeyValue>() {
                @Override
                public KeyValue reduce(KeyValue t1, KeyValue t2) throws Exception {
                    return new KeyValue(t1.getKey(), t1.getValue() + t2.getValue());
                }
            })
            .print();
    }
}
