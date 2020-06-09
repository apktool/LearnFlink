/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apktool.stream.demo.keyby;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class KeyByTuple {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        // final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);

        env.fromElements(
            new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("a", 3),
            new Tuple2<>("b", 4), new Tuple2<>("b", 5), new Tuple2<>("b", 6)
        )
            .keyBy(0)
            .map(new MapFunction<Tuple2<String, Integer>, Object>() {
                @Override
                public Object map(Tuple2<String, Integer> tuple2) throws Exception {
                    return new Tuple2<>("@" + tuple2.f0, tuple2.f1);
                }
            })
            .print();

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
