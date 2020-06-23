package com.apktool.function.builtin.scalar;

import com.apktool.common.KeyValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ArithmeticFunctions {
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

        Table result1 = tEnv.sqlQuery("SELECT LOG10(`value`) FROM " + table);
        tEnv.toAppendStream(result1, Row.class).print();

        Table result2 = tEnv.sqlQuery("SELECT PI");
        tEnv.toAppendStream(result2, Row.class).print();

        Table result3 = tEnv.sqlQuery("SELECT UUID()");
        tEnv.toAppendStream(result3, Row.class).print();

        env.execute("Flink arithmetic function Java API Skeleton");
    }
}
