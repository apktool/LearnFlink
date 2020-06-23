package com.apktool.function.udf.table;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class SplitFunctions {
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Split extends TableFunction<Tuple2<String, Integer>> {
        private String separator = " ";

        public void eval(String str) {
            for (String s : str.split(separator)) {
                collect(new Tuple2<String, Integer>(s, s.length()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<String> stream = env.fromElements(
            "li#20",
            "wang#12",
            "zhao#19",
            "zhang#29"
        );

        tEnv.registerFunction("split", new Split("#"));

        tEnv.createTemporaryView("myTable", stream, "line");
        Table result1 = tEnv.sqlQuery("SELECT line, word, length FROM myTable, LATERAL TABLE(split(line)) as T(word, length)");
        tEnv.toAppendStream(result1, Row.class).print();

        Table table = tEnv.fromDataStream(stream, "line");
        Table result2 = tEnv.sqlQuery("SELECT line, word, length FROM " + table + ", LATERAL TABLE(split(line)) as T(word, length)");
        tEnv.toAppendStream(result2, Row.class).print();

        env.execute("Flink split function Java API Skeleton");
    }
}
