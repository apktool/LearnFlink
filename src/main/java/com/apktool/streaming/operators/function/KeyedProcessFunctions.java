package com.apktool.streaming.operators.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class KeyedProcessFunctions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple2<String, Long>> data = env.addSource(new StreamDataSource()).setParallelism(1)
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Long>>(Time.milliseconds(0)) {
                @Override
                public long extractTimestamp(Tuple3<String, Long, Long> input) {
                    return input.f2;
                }
            }).map(new MapFunction<Tuple3<String, Long, Long>, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(Tuple3<String, Long, Long> input) throws Exception {
                    return new Tuple2<>(input.f0, input.f1);
                }
            });


        data.keyBy(0).process(new CountWithTimeoutFunction()).print();

        env.execute("Flink KeyedProcess Function Java API Skeleton");
    }
}
