package io.github.majusko.pulsar.collector;

import io.github.majusko.pulsar.constant.Serialization;

import java.util.Optional;

public class ProducerHolder {

    private final String topic;
    private final Class<?> clazz;
    private final Serialization serialization;
    private String cluster;

    public ProducerHolder(String topic, Class<?> clazz, Serialization serialization) {
        this.topic = topic;
        this.clazz = clazz;
        this.serialization = serialization;
    }

    public ProducerHolder(String topic, Class<?> clazz, Serialization serialization, String cluster) {
        this(topic, clazz, serialization);
        this.cluster = cluster;
    }

    public String getTopic() {
        return topic;
    }

    public Optional<String> getCluster() {
        return Optional.ofNullable(cluster);
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public Serialization getSerialization() {
        return serialization;
    }
}
