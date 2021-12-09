package com.adv.flinkjobs.source.customSource;

import com.adv.models.RandomIntOutput;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;
import java.util.Random;

public class RandomIntSourceFunction implements SourceFunction<RandomIntOutput> {
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
    public void run(SourceContext<RandomIntOutput> ctx) throws Exception {
        while (isRunning) {
            Random random = new Random();
            ctx.collect(new RandomIntOutput(random.nextInt(this.limit), new Date().getTime()));

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}


