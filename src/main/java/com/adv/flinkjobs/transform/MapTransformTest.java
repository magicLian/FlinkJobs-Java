package com.adv.flinkjobs.transform;

import com.adv.models.DeviceTemperature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapTransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //source
        DataStreamSource<String> inputStream = env.readTextFile(
                "/home/lz/workspace/flink-jobs/src/main/resources/device_temperature.txt"
        );

        //1:1
        //transform map1
        SingleOutputStreamOperator<DeviceTemperature> map1Stream = inputStream.map(new MapFunction<String, DeviceTemperature>() {
            @Override
            public DeviceTemperature map(String value) {
                String[] split = value.split(",");
                float temperature = Float.parseFloat(split[1]);
                return new DeviceTemperature(split[0], temperature, split[2]);
            }
        });

        //transform map2
        SingleOutputStreamOperator<DeviceTemperature> map2Stream = inputStream.map(new RichMapFunction<String, DeviceTemperature>() {
            @Override
            public DeviceTemperature map(String value) {
                String[] split = value.split(",");
                float temperature = Float.parseFloat(split[1]);
                return new DeviceTemperature(split[0], temperature, split[2]);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("map2 open" + parameters.toString());
            }

            @Override
            public void close() throws Exception {
                System.out.println("map2 close");
            }
        });

        map1Stream.print("map1");
        map2Stream.print("map2");

        env.execute();
    }
}
