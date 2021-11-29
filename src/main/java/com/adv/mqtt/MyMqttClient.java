package com.adv.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.Serializable;

public class MyMqttClient implements Serializable {
    String MQTT_URL;
    String MQTT_USERNAME;
    String MQTT_PASSWORD;
    String MQTT_TOPIC;
    String MQTT_CLIENTID;
    Integer MQTT_QOS;

    MqttConnectOptions options = null;
    MqttClient client = null;

    public MyMqttClient(String MQTT_URL, String MQTT_USERNAME, String MQTT_PASSWORD, String MQTT_TOPIC,
                        String MQTT_CLIENTID, Integer MQTT_QOS) {
        this.MQTT_URL = MQTT_URL;
        this.MQTT_USERNAME = MQTT_USERNAME;
        this.MQTT_PASSWORD = MQTT_PASSWORD;
        this.MQTT_TOPIC = MQTT_TOPIC;
        this.MQTT_CLIENTID = MQTT_CLIENTID;
        this.MQTT_QOS = MQTT_QOS;

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setUserName(this.MQTT_USERNAME);
        options.setPassword(this.MQTT_PASSWORD.toCharArray());
        options.setConnectionTimeout(10);
        options.setAutomaticReconnect(true);
        options.setKeepAliveInterval(60);
        this.options = options;
    }

    public void Start() {
        try {
            client = new MqttClient(this.MQTT_URL, this.MQTT_CLIENTID, new MemoryPersistence());
            client.connect(this.options);
            client.subscribe(this.MQTT_TOPIC, this.MQTT_QOS);
            client.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) {
                    System.out.println("connectionLost,reason:[" + cause.getMessage() + "]");
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    System.out.println("topic:" + topic);
                    System.out.println("message content:" + new String(message.getPayload()));
                    System.out.println("");
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                    System.out.println("deliveryComplete---------" + token.isComplete());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void Close() {
        try {
            client.close();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
