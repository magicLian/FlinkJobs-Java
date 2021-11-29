package com.adv.flinkjobs.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketTextSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //source
        DataStreamSource<String> socketTextStream = env.socketTextStream("127.0.0.1", 9000,
                "\n");

        socketTextStream.print();

        env.execute();
    }
}
