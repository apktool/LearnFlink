package com.apktool.stream.demo.windows;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author apktool
 * @package com.apktool.stream.demo.windows
 * @class WindowAssigners
 * @description TODO
 * @date 2020-06-11 22:45
 */
public class WindowAssigners {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream stream = env.socketTextStream("localhost", 9000)
            .filter(line -> line.split(",").length == 2)
            .map(line -> {
                String[] lines = line.split(",");
                return new KeyValue(lines[0], Integer.valueOf(lines[1]));
            });

        stream.keyBy("key")
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .sum("value")
            .print();

        stream.keyBy("key")
            .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .sum("value")
            .print();

        stream.keyBy("key")
            .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
            .sum("value")
            .print();

        stream.keyBy("key")
            .window(GlobalWindows.create())
            .sum("value")
            .print();

        env.execute("Flink WindowAssigners Java API Skeleton");
    }
}
