package com.adv.flinkjobs.jobs.common.source;

import com.adv.flinkjobs.jobs.models.DeviceTemperature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileReadingSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> dataStreamSource = env.readTextFile(
                "/home/magic/workspace/flink-jobs/jobs/src/main/resources/wordCount.txt");

        DataStream<List<String>> flatMapStream = dataStreamSource.flatMap(new RichFlatMapFunction<String, List<String>>() {
            @Override
            public void flatMap(String value, Collector<List<String>> out) {
                String[] split = value.split(" ");
                List<String> strings = Arrays.asList(split);
                out.collect(strings);
            }
        });

        flatMapStream.print();

        env.execute();
    }
}
