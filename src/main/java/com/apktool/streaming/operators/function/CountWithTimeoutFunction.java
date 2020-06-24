package com.apktool.streaming.operators.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String, Long>> {
// public class CountWithTimeoutFunction extends ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>> {

    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple2<String, Long> input, Context context, Collector<Tuple2<String, Long>> output) throws Exception {

        CountWithTimestamp current = state.value();
        if (current == null) {
            current = new CountWithTimestamp();
            current.key = input.f0;
        }

        current.count++;
        current.lastModified = context.timestamp();
        System.out.println("元素" + input.f0 + "进入事件时间为：" + new Date(current.lastModified));
        state.update(current);

        //注册ProcessTimer,更新一次就会有一个ProcessTimer
        context.timerService().registerEventTimeTimer(current.lastModified + 6000);
        System.out.println("定时触发时间为：" + new Date(current.lastModified + 6000));
    }

    /**
     * EventTimer被触发后产生的行为
     * 这里的timestamp是触发时间
     *
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
        //获取上次时间,与参数中的timestamp相比,如果相差等于60s 就会输出
        CountWithTimestamp res = state.value();
        System.out.println("当前时间为：" + new Date(timestamp) + res);
        if (timestamp >= res.lastModified + 6000) {
            System.out.println("定时器被触发：" + "当前时间为" + new Date(timestamp) + " 最近修改时间为" + new Date(res.lastModified));
            out.collect(new Tuple2<>(res.key, res.count));
        }
    }
}

