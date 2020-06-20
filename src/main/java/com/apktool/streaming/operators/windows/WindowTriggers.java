package com.apktool.streaming.operators.windows;

import com.apktool.common.KeyValue;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * @author apktool
 * @package com.apktool.stream.demo.windows
 * @class WindowTriggers
 * @description TODO
 * @date 2020-06-19 00:28
 */
public class WindowTriggers {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9000)
            .filter(line -> line.split(",").length == 2)
            .map(line -> {
                String[] lines = line.split(",");
                return new KeyValue(lines[0], Integer.valueOf(lines[1]));
            })
            .keyBy("key")
            .timeWindow(Time.seconds(5))
            .trigger(new Trigger<KeyValue, Window>() {
                int count = 0;

                @Override
                public TriggerResult onElement(KeyValue element, long timestamp, Window window, TriggerContext ctx) throws Exception {
                    return ++count == 3 ? TriggerResult.FIRE : TriggerResult.CONTINUE;
                }

                @Override
                public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
                    return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
                }

                @Override
                public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
                    return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
                }

                @Override
                public void clear(Window window, TriggerContext ctx) throws Exception {
                    ctx.deleteEventTimeTimer(window.maxTimestamp());
                }
            })
            .process(new ProcessWindowFunction<KeyValue, KeyValue, Tuple, TimeWindow>() {
                @Override
                public void process(Tuple tuple, Context context, Iterable<KeyValue> elements, Collector<KeyValue> out) throws Exception {
                    int sum = 0;
                    String name = "";

                    for (KeyValue element : elements) {
                        name = element.getKey();
                        sum += element.getValue();
                    }

                    out.collect(new KeyValue(name, sum));
                }
            }).print();

        env.execute("Flink WindowTriggers Java API Skeleton");
    }
}
