package com.adv.flinkjobs.jobs.common.sink;

import com.adv.flinkjobs.jobs.models.DeviceTemperature;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class JdbcSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //source
        DataStreamSource<String> inputStream = env.readTextFile(
                "/home/lz/workspace/flink-jobs/src/main/resources/device_temperature.txt"
        );

        //transform
        SingleOutputStreamOperator<DeviceTemperature> mapStream = inputStream.flatMap(new FlatMapFunction<String, DeviceTemperature>() {
            @Override
            public void flatMap(String in, Collector<DeviceTemperature> out) {
                String[] split = in.split(",");
                float temperature = Integer.parseInt(split[1]);
                out.collect(new DeviceTemperature(split[0], temperature, split[2]));
            }
        });
        mapStream.print();

        //sink
        mapStream.addSink(JdbcSink.sink("insert into menu (id, title, description) values (?, ?, ?)",
                (statement, deviceTemperature) -> {
                    statement.setString(1, deviceTemperature.getDeviceId());
                    statement.setFloat(2, deviceTemperature.getTemperature());
                    statement.setString(3, deviceTemperature.getTimestamps());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:postgresql://127.0.0.1:5432/flinkTest")
                        .withDriverName("org.postgresql.Driver")
                        .withUsername("admin")
                        .withPassword("postgres")
                        .build()
        ));

        env.execute();
    }
}
