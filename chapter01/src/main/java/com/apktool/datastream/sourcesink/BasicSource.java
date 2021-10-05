package com.apktool.datastream.sourcesink;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class BasicSource implements SourceFunction<String> {
    private final Iterable<String> elements;
    private volatile boolean isRunning;

    public BasicSource(String... elements) {
        this(Arrays.asList(elements));
    }

    public BasicSource(Iterable<String> elements) {
        this.isRunning = true;
        this.elements = elements;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        Iterator<String> iterator = elements.iterator();
        Object lock = ctx.getCheckpointLock();
        while (isRunning && iterator.hasNext()) {
            synchronized (lock) {
                ctx.collect(iterator.next());
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
