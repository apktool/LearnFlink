package com.apktool.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

public class TableToDataSet {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<Tuple2<String, Double>> text = env.fromElements(
            new Tuple2<>("li", 169.5),
            new Tuple2<>("wang", 170.0),
            new Tuple2<>("zhao", 154.1),
            new Tuple2<>("wang", 170.9),
            new Tuple2<>("zhao", 173.1)
        );

        Table table = tEnv.fromDataSet(text, "name, hight");
        Table result = tEnv.sqlQuery("SELECT name, hight FROM " + table);
        TupleTypeInfo<Tuple2<String, Double>> tupleType = new TupleTypeInfo<>(Types.STRING, Types.DOUBLE);
        DataSet<Tuple2<String, Double>> dsTuple = tEnv.toDataSet(result, tupleType);
        result.printSchema();
        dsTuple.print();

        String explanation = tEnv.explain(table);
        System.out.println(explanation);
    }
}
