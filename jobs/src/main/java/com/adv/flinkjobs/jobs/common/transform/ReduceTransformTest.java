package com.adv.flinkjobs.jobs.common.transform;

import com.adv.flinkjobs.jobs.models.DeviceTemperature;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ReduceTransformTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //source
        DataStream<String> inputStream = env.readTextFile(
                "/home/lz/workspace/flink-jobs/src/main/resources/device_temperature.txt"
        );

        //flatmap transform
        // 1:n
        KeyedStream<DeviceTemperature, String> keyedStream = inputStream.flatMap(new RichFlatMapFunction<String, DeviceTemperature>() {
            @Override
            public void flatMap(String value, Collector<DeviceTemperature> out) {
                String[] split = value.split(",");
                float temperature = Float.parseFloat(split[1]);
                out.collect(new DeviceTemperature(split[0], temperature, split[2]));
            }
        }).keyBy(new KeySelector<DeviceTemperature, String>() {
            @Override
            public String getKey(DeviceTemperature value) throws Exception {
                return value.getDeviceId();
            }
        });

        keyedStream.print("keyed");

        //reduce transform
        DataStream<DeviceTemperature> reduce = keyedStream.reduce(new ReduceFunction<DeviceTemperature>() {
            @Override
            public DeviceTemperature reduce(DeviceTemperature d1, DeviceTemperature d2) throws Exception {
                return d1.getTemperature() > d2.getTemperature()? d1 : d2;
            }
        });
        reduce.print("reduce");

        env.execute();
    }
}
