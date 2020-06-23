package com.apktool.table;

import com.apktool.common.KeyValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<KeyValue> stream = env.fromElements(
            new KeyValue("li", 20),
            new KeyValue("wang", 21),
            new KeyValue("zhao", 19),
            new KeyValue("tian", 29)
        );

        tEnv.createTemporaryView("myTable1", stream, "key, value");
        Table result1 = tEnv.from("myTable1")
            .where("key == 'li'")
            .select("key");
        tEnv.toAppendStream(result1, Row.class).print();

        tEnv.createTemporaryView("myTable2", stream, "key, value");
        Table result2 = tEnv.from("myTable2")
            .filter("value >= 20")
            .groupBy("key, value")
            .select("key, value");
        tEnv.toRetractStream(result2, Row.class).print();

        env.execute("Flink table Java API Skeleton");
    }
}
