package com.apktool.table;

import com.apktool.common.KeyValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SqlApi {
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

        Table result1 = tEnv.sqlQuery("SELECT * FROM " + table);
        tEnv.toAppendStream(result1, Row.class).print();

        Table result2 = tEnv.sqlQuery("SELECT key,COUNT(*) FROM " + table + " GROUP BY key");
        tEnv.toRetractStream(result2, Row.class).print();

        env.execute("Flink SQL Java API Skeleton");
    }
}