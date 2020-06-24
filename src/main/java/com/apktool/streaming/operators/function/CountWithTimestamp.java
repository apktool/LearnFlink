package com.apktool.streaming.operators.function;

import lombok.ToString;

@ToString
public class CountWithTimestamp {
    public String key;
    public long count;
    public long lastModified;
}
