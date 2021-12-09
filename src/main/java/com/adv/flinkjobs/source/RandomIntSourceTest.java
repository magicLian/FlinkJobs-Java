package com.adv.flinkjobs.source;

import com.adv.flinkjobs.source.customSource.RandomIntSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RandomIntSourceTest {
    public static void main(String[] args)throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        Integer limit = 30;

        DataStream<Integer> inputStream = env.addSource(new RandomIntSourceFunction(limit));

        inputStream.print();

        env.execute();
    }
}
