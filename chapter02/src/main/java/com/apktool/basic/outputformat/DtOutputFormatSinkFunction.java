package com.apktool.basic.outputformat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

public class DtOutputFormatSinkFunction<IN> extends RichSinkFunction<IN> {
    private StreamOutputFormat format;

    public DtOutputFormatSinkFunction(StreamOutputFormat format) {
        this.format = format;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        format.configure(parameters);

        StreamingRuntimeContext context = (StreamingRuntimeContext) this.getRuntimeContext();
        format.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        format.writeRecord(value);
    }

    @Override
    public void close() throws Exception {
        format.close();
    }
}
