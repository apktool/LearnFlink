package com.apktool.batch.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;

public class AggregateFunctions {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, String, Double>> text = env.fromElements(
            new Tuple3<>(1, "li", 169.5),
            new Tuple3<>(2, "wang", 170.0),
            new Tuple3<>(3, "zhao", 154.1),
            new Tuple3<>(4, "wang", 170.9),
            new Tuple3<>(5, "zhao", 173.1)
        );

        text.groupBy(1)
            .aggregate(Aggregations.SUM, 0)
            .and(Aggregations.MIN, 2)
            .print();

        text.groupBy(1)
            .minBy(0, 2)
            .print();

        text.distinct(1)
            .project(1)
            .print();
    }
}
