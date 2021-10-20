package com.apktool.table.basic;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class BasicDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Person> stone = env.fromElements(
            new Person("Fred", 35),
            new Person("Wilma", 32),
            new Person("Alias", 2)
        );

        Table sourceTable = tEnv.fromDataStream(stone);
        sourceTable.printSchema();

        tEnv.createTemporaryView("table1", sourceTable);
        TableResult result = tEnv.sqlQuery("select * from table1").execute();
        result.print();
    }

    private static class Person {
        private String name;
        private Integer age;

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "name=" + name + "," + "age=" + age;
        }
    }
}
