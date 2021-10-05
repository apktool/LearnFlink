package com.apktool.datastream.basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BasicDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Person> stone = env.fromElements(
            new Person("Fred", 35),
            new Person("Wilma", 32),
            new Person("Alias", 2)
        );

        DataStream<Person> adult = stone.filter(
            new FilterFunction<Person>() {
                @Override
                public boolean filter(Person person) throws Exception {
                    return person.getAge() > 18;
                }
            }
        );

        adult.print();
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
