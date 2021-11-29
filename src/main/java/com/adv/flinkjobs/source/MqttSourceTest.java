package com.adv.flinkjobs.source;

import com.adv.flinkjobs.source.customSource.MqttSourceFunction;
import com.adv.models.MqttMsg;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MqttSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source
        String MQTT_URL = "tcp://127.0.0.1:1883";
        String MQTT_USERNAME = "admin";
        String MQTT_PASSWORD = "admin";
        String MQTT_CLIENTID = "mqttSourceFunction";
        String MQTT_TOPIC = "HELM/up/#";
        Integer MQTT_QOS = 0;

        DataStreamSource<MqttMsg> dataStreamSource = env.addSource(
                new MqttSourceFunction(MQTT_URL, MQTT_USERNAME, MQTT_PASSWORD, MQTT_TOPIC, MQTT_CLIENTID, MQTT_QOS)
        );

        dataStreamSource.print();

        env.execute();
    }
}

