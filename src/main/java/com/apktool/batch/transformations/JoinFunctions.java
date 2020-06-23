package com.apktool.batch.transformations;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class JoinFunctions {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, String, Double>> text1 = env.fromElements(
            new Tuple3<>(1, "li", 169.5),
            new Tuple3<>(2, "wang", 170.0),
            new Tuple3<>(3, "zhao", 154.1),
            new Tuple3<>(4, "wang", 170.9),
            new Tuple3<>(5, "zhao", 173.1)
        );

        DataSet<Tuple3<Integer, String, Double>> text2 = env.fromElements(
            new Tuple3<>(1, "li", 169.5),
            new Tuple3<>(2, "chen", 170.0),
            new Tuple3<>(3, "lu", 154.1),
            new Tuple3<>(4, "ai", 170.9),
            new Tuple3<>(5, "liao", 173.1)
        );

        text1.join(text2)
            .where(0)
            .equalTo(0)
            .print();

        text1.joinWithTiny(text2)
            .where(0)
            .equalTo(0)
            .print();

        text1.join(text2, JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST)
            .where(0)
            .equalTo(0)
            .print();

        text1.join(text2)
            .where(0)
            .equalTo(0)
            .with(new JoinFunction<Tuple3<Integer, String, Double>, Tuple3<Integer, String, Double>, Tuple2<Integer, Double>>() {
                @Override
                public Tuple2<Integer, Double> join(Tuple3<Integer, String, Double> t1, Tuple3<Integer, String, Double> t2) throws Exception {
                    return new Tuple2<>(t1.f0, (t1.f2 + t2.f2) / 2.0);
                }
            })
            .print();
    }
}
