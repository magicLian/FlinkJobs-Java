package com.adv.flinkjobs.jobs.common.sink;

import com.adv.flinkjobs.jobs.models.DeviceTemperature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;

import java.net.URL;

public class PrintSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //source
//        URL resource = PrintSinkTest.class.getResource("device_temperature.txt");
        DataStreamSource<String> inputStream = env.readTextFile(
                "/home/magic/workspace/flink-jobs/jobs/target/classes/device_temperature.txt");

        //transform
        DataStream<DeviceTemperature> mapStream = inputStream.flatMap(new FlatMapFunction<String, DeviceTemperature>() {
            @Override
            public void flatMap(String in, Collector<DeviceTemperature> out) {
                String[] split = in.split(",");
                float temperature = Float.parseFloat(split[1]);
                out.collect(new DeviceTemperature(split[0], temperature, split[2]));
            }
        });
//        mapStream.print();

        //sink
        mapStream.addSink(new PrintSinkFunction<>());

        env.execute();
    }
}
