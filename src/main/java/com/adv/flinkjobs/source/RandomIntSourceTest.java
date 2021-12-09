package com.adv.flinkjobs.source;

import com.adv.flinkjobs.source.customSource.RandomIntSourceFunction;
import com.adv.models.RandomIntOutput;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class RandomIntSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Integer limit = 30;

        //add source
        DataStream<RandomIntOutput> inputStream = env.addSource(new RandomIntSourceFunction(limit));

        //make timestamps and watermark
        DataStream<RandomIntOutput> timestampsStream = inputStream.assignTimestampsAndWatermarks(
            WatermarkStrategy
                .<RandomIntOutput>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    new SerializableTimestampAssigner<RandomIntOutput>() {
                        @Override
                        public long extractTimestamp(RandomIntOutput element, long recordTimestamp) {
                            return element.getTs();
                        }
                    }
                )
        );

        inputStream.print("source");
        timestampsStream.print("timestamp");

        env.execute();
    }
}
