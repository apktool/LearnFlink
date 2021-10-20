package com.apktool.medium.inputformat;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

public class StreamInputFormat extends RichInputFormat<RowData, InputSplit> {
    private String[] data = null;
    private static volatile AtomicInteger idx = new AtomicInteger(0);

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
    public RowData nextRecord(RowData input) throws IOException {
        GenericRowData row = new GenericRowData(input.getArity());

        String[] tmp = data[idx.getAndIncrement()].split(",");
        row.setField(0, StringData.fromString(tmp[0]));
        row.setField(1, Integer.valueOf(tmp[1]));

        return row;
    }

    @Override
    public void close() throws IOException {
        data = null;
    }
}
