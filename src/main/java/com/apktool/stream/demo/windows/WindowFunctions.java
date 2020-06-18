package com.apktool.stream.demo.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author apktool
 * @package com.apktool.stream.demo.windows
 * @class WindowFunctions
 * @description TODO
 * @date 2020-06-18 00:35
 */
public class WindowFunctions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WindowedStream<KeyValue, Tuple, TimeWindow> stream = env.socketTextStream("localhost", 9000)
            .filter(line -> line.split(",").length == 2)
            .map(line -> {
                String[] lines = line.split(",");
                return new KeyValue(lines[0], Integer.valueOf(lines[1]));
            })
            .keyBy("key")
            .timeWindow(Time.seconds(5));

        // sum
        stream.reduce(new ReduceFunction<KeyValue>() {
            @Override
            public KeyValue reduce(KeyValue t1, KeyValue t2) throws Exception {
                return new KeyValue(t1.getKey(), t1.getValue() + t2.getValue());
            }
        }).print();

        // avg
        stream.aggregate(new AggregateFunction<KeyValue, Tuple3<String, Integer, Integer>, KeyValue>() {
            @Override
            public Tuple3<String, Integer, Integer> createAccumulator() {
                return new Tuple3<>("", 0, 0);
            }

            @Override
            public Tuple3<String, Integer, Integer> add(KeyValue keyValue, Tuple3<String, Integer, Integer> accumulator) {
                return new Tuple3<>(keyValue.getKey(), accumulator.f1 + keyValue.getValue(), accumulator.f2 + 1);
            }

            @Override
            public KeyValue getResult(Tuple3<String, Integer, Integer> accumulator) {
                return new KeyValue(accumulator.f0, accumulator.f1 / accumulator.f2);
            }

            @Override
            public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> a, Tuple3<String, Integer, Integer> b) {
                return new Tuple3<>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
            }
        }).print();


        // count
        stream.process(new ProcessWindowFunction<KeyValue, KeyValue, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<KeyValue> elements, Collector<KeyValue> out) throws Exception {
                int count = 0;
                String name = "";
                for (KeyValue element : elements) {
                    name = element.getKey();
                    count++;
                }

                out.collect(new KeyValue(name, count));
            }
        }).print();

        env.execute("Flink WindowFunctions Java API Skeleton");
    }
}
