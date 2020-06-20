package com.apktool.stream.demo.windows;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author apktool
 * @package com.apktool.stream.demo.windows
 * @class WindowEvictors
 * @description https://blog.csdn.net/qq_41323538/article/details/103585906
 * @date 2020-06-19 00:47
 */
public class WindowEvictors {
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


        stream.evictor(CountEvictor.of(2))
            .process(new ProcessWindowFunction<KeyValue, KeyValue, Tuple, TimeWindow>() {
                @Override
                public void process(Tuple tuple, Context context, Iterable<KeyValue> elements, Collector<KeyValue> out) throws Exception {
                    String name = "";
                    int count = 0;

                    for (KeyValue element : elements) {
                        name = element.getKey();
                        count++;
                    }

                    out.collect(new KeyValue(name, count));
                }
            }).print();


        stream.evictor(new Evictor<KeyValue, TimeWindow>() {
            private long gantryFootSize = 20000L;
            private long time = System.currentTimeMillis() + 15000L;

            @Override
            public void evictBefore(Iterable<TimestampedValue<KeyValue>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {
                evict(iterable, i, timeWindow, evictorContext);
            }

            @Override
            public void evictAfter(Iterable<TimestampedValue<KeyValue>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {
                evict(iterable, i, timeWindow, evictorContext);
            }

            public void evict(Iterable<TimestampedValue<KeyValue>> elements, int size, TimeWindow timeWindow, EvictorContext evictorContext) {
                long evictCutBegin = timeWindow.getStart() + gantryFootSize;
                long evictCutEnd = timeWindow.getEnd() - gantryFootSize;

                Iterator<TimestampedValue<KeyValue>> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    if (time >= evictCutBegin && time <= evictCutEnd) {
                        System.out.println(iterator.next().getValue());
                    }
                }
            }
        }).reduce(new ReduceFunction<KeyValue>() {
            @Override
            public KeyValue reduce(KeyValue t1, KeyValue t2) throws Exception {
                return new KeyValue(t1.getKey(), t1.getValue() + t2.getValue());
            }
        }).print();

        env.execute("Flink WindowEvictors Java API Skeleton");
    }
}
