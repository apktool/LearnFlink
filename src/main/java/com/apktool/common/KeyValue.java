package com.apktool.common;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author apktool
 * @package com.apktool.streaming.keyby
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
