package com.apktool.streaming.operators.joining;

import com.apktool.common.KeyValue;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author apktool
 * @package com.apktool.streaming.joining
 * @class TumblingWindowJoin
 * @description TODO
 * @date 2020-06-20 09:11
 */
public class TumblingWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream stream1 = env.socketTextStream("localhost", 9000)
            .filter(line -> line.split(",").length == 2)
            .map(line -> {
                String[] lines = line.split(",");
                return new KeyValue(lines[0], Integer.valueOf(lines[1]));
            });

        DataStream stream2 = env.socketTextStream("localhost", 9001)
            .filter(line -> line.split(",").length == 2)
            .map(line -> {
                String[] lines = line.split(",");
                return new KeyValue(lines[0], Integer.valueOf(lines[1]));
            });

        stream1.join(stream2)
            .where(new KeySelector<KeyValue, Object>() {
                @Override
                public Object getKey(KeyValue keyValue) throws Exception {
                    return keyValue.getKey();
                }
            })
            .equalTo(new KeySelector<KeyValue, Object>() {

                @Override
                public Object getKey(KeyValue keyValue) throws Exception {
                    return keyValue.getKey();
                }
            })
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .apply(new JoinFunction<KeyValue, KeyValue, Tuple3<String, Integer, Integer>>() {
                @Override
                public Tuple3<String, Integer, Integer> join(KeyValue first, KeyValue second) throws Exception {
                    return new Tuple3<>(first.getKey(), first.getValue(), second.getValue());
                }
            }).print();

        env.execute("Flink Tumbling Window Join Java API Skeleton");
    }
}
