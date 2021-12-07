package com.adv.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class Jarlib {
    public static class substring extends ScalarFunction {
        public String eval(String s, Integer begin, Integer end) {
            return s.substring(begin, end);
        }
    }
}
