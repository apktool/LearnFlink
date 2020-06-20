package com.apktool.streaming.operators.windows;

import com.apktool.common.KeyValue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Iterator;
import java.util.Map;

public class WindowSideOutput {
    public static void main(String[] args) throws Exception {
        Map<String, OutputTag<KeyValue>> tag = Maps.newHashMap();
        tag.put("a", new OutputTag<>("a", TypeInformation.of(KeyValue.class)));
        tag.put("b", new OutputTag<>("b", TypeInformation.of(KeyValue.class)));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<KeyValue> stream = env.socketTextStream("localhost", 9000)
            .filter(line -> line.split(",").length == 2)
            .map(line -> {
                String[] lines = line.split(",");
                return new KeyValue(lines[0], Integer.valueOf(lines[1]));
            })
            .keyBy("key")
            .timeWindow(Time.seconds(5))
            .process(new ProcessWindowFunction<KeyValue, KeyValue, Tuple, TimeWindow>() {

                @Override
                public void process(Tuple tuple, Context context, Iterable<KeyValue> elements, Collector<KeyValue> collector) throws Exception {
                    Iterator<KeyValue> iterable = elements.iterator();
                    while (iterable.hasNext()) {
                        KeyValue keyValue = iterable.next();
                        switch (keyValue.getKey()) {
                            case "a":
                                context.output(tag.get("a"), keyValue);
                                break;
                            case "b":
                                context.output(tag.get("b"), keyValue);
                                break;
                            default:
                                collector.collect(keyValue);

                        }
                    }
                }
            });

        stream.getSideOutput(tag.get("a")).print();
        stream.getSideOutput(tag.get("b")).print();
        stream.print();

        env.execute("Flink WindowSideOutput Java API Skeleton");
    }
}
