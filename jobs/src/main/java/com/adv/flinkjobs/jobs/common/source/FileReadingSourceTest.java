package com.adv.flinkjobs.jobs.common.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileReadingSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> dataStreamSource = env.readTextFile("/home/lz/workspace/flink-jobs/src/main/resources/wordCount.txt");

        dataStreamSource.print();

        env.execute();
    }
}
