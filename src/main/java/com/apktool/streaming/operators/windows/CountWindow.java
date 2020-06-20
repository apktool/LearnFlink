package com.apktool.streaming.operators.windows;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author apktool
 * @package com.apktool.stream.demo.windows
 * @class CountWindow
 * @description TODO
 * @date 2020-06-11 22:32
 */
public class CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream stream = env.socketTextStream("localhost", 9000)
            .filter(line -> line.split(",").length == 2)
            .map(line -> {
                String[] lines = line.split(",");
                return new KeyValue(lines[0], Integer.valueOf(lines[1]));
            })
            .keyBy("key")
            .countWindow(5)
            .sum("value");
        stream.print();

        env.execute("Flink Window Java API Skeleton");
    }
}
