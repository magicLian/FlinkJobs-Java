package com.adv.flinkjobs.jobs.models;

import java.nio.charset.StandardCharsets;

public class MqttMsg {
    private String topic;

    private byte[] payload;

    private Integer qos;

    private Integer messageId;

    public MqttMsg(String topic, byte[] payload, Integer qos, Integer messageId) {
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
        this.messageId = messageId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public Integer getQos() {
        return qos;
    }

    public void setQos(Integer qos) {
        this.qos = qos;
    }

    public Integer getMessageId() {
        return messageId;
    }

    public void setMessageId(Integer messageId) {
        this.messageId = messageId;
    }

    @Override
    public String toString() {
        return "MqttMsg{" +
                "topic='" + topic + '\'' +
                ", payload=" + new String(payload, StandardCharsets.UTF_8) +
                ", qos=" + qos +
                ", messageId=" + messageId +
                '}';
    }
}
