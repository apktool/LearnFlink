package com.apktool.stream.demo.windows;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author apktool
 * @package com.apktool.stream.demo.windows
 * @class TumblingWindow
 * @description TODO
 * @date 2020-06-11 22:45
 */
public class CountAllWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream stream = env.socketTextStream("localhost", 9000)
            .filter(line -> line.split(",").length == 2)
            .map(line -> {
                String[] lines = line.split(",");
                return new KeyValue(lines[0], Integer.valueOf(lines[1]));
            })
            .countWindowAll(5)
            .sum("value");

        stream.print();

        env.execute("Flink Window Java API Skeleton");
    }
}
