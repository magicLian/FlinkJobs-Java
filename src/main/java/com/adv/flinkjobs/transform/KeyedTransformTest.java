package com.adv.flinkjobs.transform;

import com.adv.models.DeviceTemperature;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyedTransformTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //source
        DataStream<String> inputStream = env.readTextFile(
                "/home/lz/workspace/flink-jobs/src/main/resources/device_temperature.txt"
        );

        //map transform
        // 1:1
        KeyedStream<DeviceTemperature, Tuple> keyedStream = inputStream.flatMap(new RichFlatMapFunction<String, DeviceTemperature>() {
                    @Override
                    public void flatMap(String value,Collector<DeviceTemperature> out) {
                        String[] split = value.split(",");
                        float temperature = Float.parseFloat(split[1]);
                        out.collect(new DeviceTemperature(split[0], temperature, split[2]));
                    }
                }).keyBy("deviceId");

        keyedStream.print("keyed");

        env.execute();
    }

}
