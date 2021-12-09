package com.adv.flinkjobs.source.customSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomIntSourceFunction implements SourceFunction<Integer> {
    private static final long serialVersionUID = 1L;

    private Integer limit;

    private volatile boolean isRunning = true;
    private transient Object waitLock;

    public RandomIntSourceFunction() {
    }

    public RandomIntSourceFunction(Integer limit) {
        this.limit = limit;
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (isRunning) {
            Random random = new Random();
            ctx.collect(random.nextInt(this.limit));

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}


