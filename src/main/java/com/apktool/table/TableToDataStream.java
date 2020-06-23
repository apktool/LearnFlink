package com.apktool.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableToDataStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple2<String, Integer>> stream = env.fromElements(
            new Tuple2<>("li", 20),
            new Tuple2<>("wang", 21),
            new Tuple2<>("zhao", 19),
            new Tuple2<>("tian", 29)
        );

        Table table = tEnv.fromDataStream(stream, "key, value");

        Table result1 = tEnv.sqlQuery("SELECT * FROM " + table);
        DataStream<Row> dsRow = tEnv.toAppendStream(result1, Row.class);
        dsRow.print();

        TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING, Types.INT);
        Table result2 = tEnv.sqlQuery("SELECT * FROM " + table);
        DataStream<Tuple2<String, Integer>> dsTuple = tEnv.toAppendStream(result2, tupleType);
        dsTuple.print();

        env.execute("Flink Table to DataStream Java API Skeleton");
    }
}
