package com.apktool.table.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamToTable {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        TypeInformation<String> info = TypeInformation.of(new TypeHint<>() {
        });

        DataStream<Person> stone = env.addSource(
            new SocketTextStreamFunction("localhost", 9999, "\n", 0), info

        )
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String s) throws Exception {
                    return s.split(",").length == 2;
                }
            })
            .map(
                new MapFunction<String, Person>() {
                    @Override
                    public Person map(String s) throws Exception {
                        String[] tmp = s.split(",");
                        String name = tmp[0];
                        Integer age = Integer.valueOf(tmp[1]);
                        return new Person(name, age);
                    }
                }
            );

        Table sourceTable = tEnv.fromDataStream(stone);
        tEnv.createTemporaryView("table1", sourceTable);
        TableResult result = tEnv.sqlQuery("select * from table1").execute();

        result.print();
        env.execute();
    }

    private static class Person {
        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        private String name;
        private Integer age;

        public String getName() {
            return name;
        }

        public Integer getAge() {
            return age;
        }

        @Override
        public String toString() {
            return "name=" + name + "," + "age=" + age;
        }
    }
}
