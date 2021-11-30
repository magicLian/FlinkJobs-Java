package com.adv.flinkjobs.transform;

import com.adv.models.DeviceTemperature;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class FlatMapTransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        DataStreamSource<String> inputStream = env.readTextFile(
                "/home/lz/workspace/flink-jobs/src/main/resources/device_temperature.txt"
        );

        //transform 1:n
        DataStream<DeviceTemperature> flatMapStream = inputStream.flatMap(new RichFlatMapFunction<String, DeviceTemperature>() {
            @Override
            public void flatMap(String value, Collector<DeviceTemperature> out) {
                String[] split = value.split(",");
                float temperature = Float.parseFloat(split[1]);
                out.collect(new DeviceTemperature(split[0], temperature, split[2]));
            }
        });

        flatMapStream.print();

        env.execute();
    }
}
