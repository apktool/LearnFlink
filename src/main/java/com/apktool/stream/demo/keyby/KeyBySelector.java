package com.apktool.stream.demo.keyby;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author apktool
 * @package com.apktool.stream.demo.keyby
 * @class KeyBySelector
 * @description TODO
 * @date 2020-06-09 23:47
 */
public class KeyBySelector {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(
            new KeyValue("a", 1), new KeyValue("a", 2), new KeyValue("a", 3),
            new KeyValue("b", 4), new KeyValue("b", 5), new KeyValue("b", 6)
        )
            .keyBy(new KeySelector<KeyValue, Object>() {
                @Override
                public Object getKey(KeyValue keyValue) throws Exception {
                    return keyValue.getKey();
                }
            })
            .map(new MapFunction<KeyValue, KeyValue>() {
                @Override
                public KeyValue map(KeyValue keyValue) throws Exception {
                    return new KeyValue("@" + keyValue.getKey(), keyValue.getValue());
                }
            })
            .print();

        env.execute("Flink Streaming Java API Skeleton");
    }
}
