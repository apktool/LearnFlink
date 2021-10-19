package com.apktool.basic.inputformat;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

public class StreamInputFormat extends RichInputFormat<Tuple2<String, Integer>, InputSplit> {
    private String[] data = null;
    private static AtomicInteger idx = new AtomicInteger(0);

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return new BaseStatistics() {
            @Override
            public long getTotalInputSize() {
                return data.length;
            }

            @Override
            public long getNumberOfRecords() {
                return BaseStatistics.NUM_RECORDS_UNKNOWN;
            }

            @Override
            public float getAverageRecordWidth() {
                return BaseStatistics.NUM_RECORDS_UNKNOWN;
            }
        };
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        InputSplit[] inputSplits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            inputSplits[i] = new GenericInputSplit(i, minNumSplits);
        }

        return inputSplits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        data = new String[]{
            "li,21", "wang,22", "gao,32", "zhao,12"
        };
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return idx.get() == data.length;
    }

    @Override
    public Tuple2<String, Integer> nextRecord(Tuple2<String, Integer> tuple2) throws IOException {
        String[] tmp = data[idx.getAndIncrement()].split(",");
        tuple2.setFields(tmp[0], Integer.valueOf(tmp[1]));
        return tuple2;
    }

    @Override
    public void close() throws IOException {
        data = null;
    }
}
