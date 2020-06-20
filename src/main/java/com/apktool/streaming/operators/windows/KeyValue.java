package com.apktool.streaming.operators.windows;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author apktool
 * @package com.apktool.stream.demo.keyby
 * @class KeyValue
 * @description TODO
 * @date 2020-06-09 22:41
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyValue {
    private String key;
    private Integer value;
}
