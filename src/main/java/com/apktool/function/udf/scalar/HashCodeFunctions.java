package com.apktool.function.udf.scalar;

import com.apktool.common.KeyValue;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class HashCodeFunctions {
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HashCode extends ScalarFunction {
        private int factor = 12;

        public int eval(String s) {
            return s.hashCode() * factor;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<KeyValue> stream = env.fromElements(
            new KeyValue("li", 20),
            new KeyValue("wang", 21),
            new KeyValue("zhao", 19),
            new KeyValue("li", 29)
        );

        tEnv.registerFunction("hashCode", new HashCode(10));

        Table table = tEnv.fromDataStream(stream, "key, value");
        Table result1 = tEnv.sqlQuery("SELECT hashCode(key) FROM " + table);
        tEnv.toAppendStream(result1, Row.class).print();

        env.execute("Flink hashcode function Java API Skeleton");
    }
}
