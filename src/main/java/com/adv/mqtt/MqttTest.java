package com.adv.mqtt;

public class MqttTest {
    public static void main(String[] args) {
        //source
        String MQTT_URL = "tcp://127.0.0.1:1883";
        String MQTT_USERNAME = "admin";
        String MQTT_PASSWORD = "admin";
        String MQTT_CLIENTID = "mqttTest";
        String MQTT_TOPIC = "HELM/up/#";
        Integer MQTT_QOS = 0;

        MyMqttClient client = new MyMqttClient(MQTT_URL, MQTT_USERNAME, MQTT_PASSWORD, MQTT_TOPIC, MQTT_CLIENTID, MQTT_QOS);
        client.Start();
    }
}

