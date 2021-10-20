package com.apktool.medium.inputformat;

import java.util.Arrays;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public class StreamDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamInputFormat inputFormat = new StreamInputFormat();

        TypeInformation<RowData> info = getTypeInformation(
            new DataType[]{DataTypes.STRING(), DataTypes.INT()},
            new String[]{"name", "age"}
        );

        DtInputFormatSourceFunction<RowData> function = new DtInputFormatSourceFunction<>(inputFormat, info);
        env.addSource(function, "demo", info)
            .map(t -> new Person(t.getString(0).toString(), t.getInt(1)))
            .filter(t -> t.age > 18)
            .map(t -> String.format("%s:%s", t.name, t.age))
            .print();

        env.execute();
    }

    public static TypeInformation<RowData> getTypeInformation(DataType[] dataTypes, String[] fieldNames) {
        return InternalTypeInfo.of(getRowType(dataTypes, fieldNames));
    }

    public static RowType getRowType(DataType[] dataTypes, String[] fieldNames) {
        return RowType.of(
            Arrays.stream(dataTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new),
            fieldNames
        );
    }

    private static class Person {
        protected Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        private String name;
        private Integer age;
    }
}
