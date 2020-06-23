package com.apktool.function.builtin.scalar;

import com.apktool.common.KeyValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class LogicalFunctions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<KeyValue> stream = env.fromElements(
            new KeyValue("li", 20),
            new KeyValue("wang", 21),
            new KeyValue("zhao", 19),
            new KeyValue("li", 29)
        );

        Table table = tEnv.fromDataStream(stream, "key, value");

        Table result1 = tEnv.sqlQuery("SELECT * FROM " + table + " WHERE key = 'li' AND `value` = 20");
        tEnv.toAppendStream(result1, KeyValue.class).print();
        tEnv.toAppendStream(result1, Row.class).print();

        Table result2 = tEnv.sqlQuery("SELECT key IN ('li', 'wang') IS TRUE FROM " + table);
        tEnv.toAppendStream(result2, Row.class).print();

        env.execute("Flink logical function Java API Skeleton");
    }
}
