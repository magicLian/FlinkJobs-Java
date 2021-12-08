package com.adv.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class Substring extends ScalarFunction {
    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, end);
    }
}
