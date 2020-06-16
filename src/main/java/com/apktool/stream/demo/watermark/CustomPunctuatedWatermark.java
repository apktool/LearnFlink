package com.apktool.stream.demo.watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

/**
 * @author apktool
 * @package com.apktool.stream.demo.watermark
 * @class CustomPeriodicWatermark
 * @description TODO
 * @date 2020-06-17 00:22
 */
public class CustomPunctuatedWatermark {
    public static void main(String[] args) throws Exception {
        List<Tuple3<Long, String, Integer>> list = Arrays.asList(
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
        );

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple3<Long, String, Integer>> stream = env.fromCollection(list)
            .assignTimestampsAndWatermarks(
                new PunctuatedAssigner()
            );

        Table table = tEnv.fromDataStream(stream, "t.rowtime, name, score");
        Table result = tEnv.sqlQuery(
            "SELECT TUMBLE_START(t, INTERVAL '5' SECOND) AS window_start, " +
                " TUMBLE_END(t, INTERVAL '5' SECOND) AS window_end, " +
                " SUM(score) " +
                " FROM " + table +
                " GROUP BY TUMBLE(t, INTERVAL '5' SECOND)"
        );

        tEnv.toAppendStream(result, Row.class).print();

        env.execute("Flink table Java API Skeleton");

    }


    static class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<Tuple3<Long, String, Integer>> {
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Tuple3<Long, String, Integer> lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp);
        }

        @Override
        public long extractTimestamp(Tuple3<Long, String, Integer> element, long previousElementTimestamp) {
            return element.f0;
        }
    }
}
