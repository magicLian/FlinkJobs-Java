package com.adv.pyudf.model;

public class DeviceTemperature {
    private String deviceId;
    private float temperature;
    private String timestamps;

    public DeviceTemperature(String deviceId, float temperature, String timestamps) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.timestamps = timestamps;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    public String getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(String timestamps) {
        this.timestamps = timestamps;
    }

    @Override
    public String toString() {
        return "DeviceTemperature{" +
                "deviceId='" + deviceId + '\'' +
                ", temperature=" + temperature +
                ", timestamps='" + timestamps + '\'' +
                '}';
    }
}