package com.apktool.streaming.operators.joining;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author apktool
 * @package com.apktool.streaming.operators.joining
 * @class IntervalJoin
 * @description TODO
 * @date 2020-06-20 18:05
 */
public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream stream1 = env.fromElements(
            // 2020-06-13 17:50:34
            new Tuple3<>(1592041834851L, "qian", 204),
            // 2020-06-13 17:50:36
            new Tuple3<>(1592041836801L, "tian", 208),
            // 2020-06-13 17:50:40
            new Tuple3<>(1592041840623L, "sun", 218),
            // 2020-06-13 17:50:46
            new Tuple3<>(1592041846191L, "zhao", 303),
            // 2020-06-13 17:50:45
            new Tuple3<>(1592041845215L, "wang", 300)
        )
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                        return element.f0;
                    }
                }
            );


        DataStream stream2 = env.fromElements(
            // 2020-06-13 17:50:34
            new Tuple3<>(1592041834851L, "li", 204),
            // 2020-06-13 17:50:36
            new Tuple3<>(1592041836801L, "tian", 208),
            // 2020-06-13 17:50:40
            new Tuple3<>(1592041840623L, "sun", 218),
            // 2020-06-13 17:50:46
            new Tuple3<>(1592041846191L, "zhang", 303),
            // 2020-06-13 17:50:45
            new Tuple3<>(1592041845215L, "wang", 300)
        )
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                        return element.f0;
                    }
                }
            );


        stream1.keyBy(1).intervalJoin(stream2.keyBy(1))
            .between(Time.seconds(-2), Time.seconds(2))
            .process(new ProcessJoinFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>, Tuple3<String, Integer, Integer>>() {

                @Override
                public void processElement(Tuple3<Long, String, Integer> left, Tuple3<Long, String, Integer> right, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                    Tuple3<String, Integer, Integer> tuple3 = new Tuple3(left.f1, left.f2, right.f2);
                    out.collect(tuple3);
                }
            })
            .print();

        env.execute("Flink Interval Window Join Java API Skeleton");
    }
}
