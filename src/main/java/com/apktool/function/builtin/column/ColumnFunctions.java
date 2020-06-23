package com.apktool.function.builtin.column;

import com.apktool.common.KeyValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ColumnFunctions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<KeyValue> stream = env.fromElements(
            new KeyValue("li", 20),
            new KeyValue("wang", 21),
            new KeyValue("zhao", 19),
            new KeyValue("li", 29)
        );

        tEnv.createTemporaryView("myTable", stream);
        Table result1 = tEnv.from("myTable").select("withColumns(*)");
        tEnv.toAppendStream(result1, Row.class).print();

        env.execute("Flink column function Java API Skeleton");
    }
}
