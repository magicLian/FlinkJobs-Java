package com.adv.flinkjobs.jobs.common.source.customSource;

import com.adv.flinkjobs.jobs.models.MqttMsg;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttSourceFunction implements SourceFunction<MqttMsg> {
    private static final long serialVersionUID = 1L;

    String MQTT_URL;
    String MQTT_USERNAME;
    String MQTT_PASSWORD;
    String MQTT_TOPIC;
    String MQTT_CLIENTID;
    Integer MQTT_QOS;

    private volatile boolean isRunning = true;
    private transient MqttClient client;
    private transient Object waitLock;

    public MqttSourceFunction(String MQTT_URL, String MQTT_USERNAME, String MQTT_PASSWORD, String MQTT_TOPIC, String MQTT_CLIENTID, Integer MQTT_QOS) {
        this.MQTT_URL = MQTT_URL;
        this.MQTT_USERNAME = MQTT_USERNAME;
        this.MQTT_PASSWORD = MQTT_PASSWORD;
        this.MQTT_TOPIC = MQTT_TOPIC;
        this.MQTT_CLIENTID = MQTT_CLIENTID;
        this.MQTT_QOS = MQTT_QOS;
    }

    @Override
    public void run(SourceContext<MqttMsg> ctx) throws Exception {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setUserName(this.MQTT_USERNAME);
        options.setPassword(this.MQTT_PASSWORD.toCharArray());
        options.setConnectionTimeout(10);
        options.setAutomaticReconnect(true);
        options.setKeepAliveInterval(60);

        if (isRunning) {
            client = new MqttClient(this.MQTT_URL, this.MQTT_CLIENTID, new MemoryPersistence());
            client.connect(options);
            client.subscribe(this.MQTT_TOPIC, this.MQTT_QOS);
            client.setCallback(new MqttCallback() {
                public void connectionLost(Throwable cause) {
                    System.out.println("connectionLost,reason:[" + cause.getMessage() + "]");
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    System.out.println("topic:" + topic);
                    System.out.println("message content:" + new String(message.getPayload()));
                    ctx.collect(new MqttMsg(topic, message.getPayload(), message.getQos(), message.getId()));
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                    System.out.println("deliveryComplete" + token.isComplete());
                }
            });

            waitLock = new Object();

            //this will stuck the `run` function and continuous gather the mqtt data.
            while (isRunning) {
                synchronized (waitLock) {
                    waitLock.wait(100L);
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;

        if (this.client.isConnected()) {
            try {
                this.client.disconnect();
            } catch (MqttException e) {

            }
        }
    }
}
