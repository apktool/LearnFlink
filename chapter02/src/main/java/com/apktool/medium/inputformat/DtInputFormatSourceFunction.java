package com.apktool.medium.inputformat;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DtInputFormatSourceFunction<OUT> extends InputFormatSourceFunction<OUT> implements CheckpointedFunction {
    private static final Logger log = LoggerFactory.getLogger(com.apktool.basic.inputformat.DtInputFormatSourceFunction.class);

    private final InputFormat<OUT, InputSplit> format;

    private final TypeInformation<OUT> typeInfo;

    private transient InputSplitProvider provider;
    private transient TypeSerializer<OUT> serializer;

    private volatile boolean isRunning = true;

    public DtInputFormatSourceFunction(InputFormat<OUT, ?> format, TypeInformation<OUT> typeInfo) {
        super(format, typeInfo);
        this.format = (InputFormat<OUT, InputSplit>) format;
        this.typeInfo = typeInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

        format.configure(parameters);
        provider = context.getInputSplitProvider();
        serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
    }

    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        if (isRunning && format instanceof RichInputFormat) {
            ((RichInputFormat) format).openInputFormat();
        }

        InputSplit split = provider.getNextInputSplit(getRuntimeContext().getUserCodeClassLoader());
        OUT nextElement = serializer.createInstance();

        format.open(split);
        while (isRunning && !format.reachedEnd()) {
            nextElement = format.nextRecord(nextElement);
            if (nextElement != null) {
                ctx.collect(nextElement);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        format.close();
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.info(context.toString());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        log.info(context.toString());
    }
}
