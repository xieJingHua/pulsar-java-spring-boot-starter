package io.github.majusko.pulsar.producer;

import io.github.majusko.pulsar.PulsarClientContainer;
import io.github.majusko.pulsar.annotation.PulsarProducer;
import io.github.majusko.pulsar.collector.ProducerHolder;
import io.github.majusko.pulsar.error.exception.ProducerInitException;
import io.github.majusko.pulsar.utils.SchemaUtils;
import io.github.majusko.pulsar.utils.UrlBuildService;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.stereotype.Component;
import org.springframework.util.StringValueResolver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class ProducerCollector implements BeanPostProcessor, EmbeddedValueResolverAware {

    private final UrlBuildService urlBuildService;

    private final Map<String, Producer> producers = new ConcurrentHashMap<>();

    private StringValueResolver stringValueResolver;
    private ProducerInterceptor producerInterceptor;

    private final PulsarClientContainer clientContainer;

    public ProducerCollector(PulsarClientContainer clientContainer, UrlBuildService urlBuildService, ProducerInterceptor producerInterceptor) {
        this.clientContainer = clientContainer;
        this.urlBuildService = urlBuildService;
        this.producerInterceptor = producerInterceptor;
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) {
        final Class<?> beanClass = bean.getClass();

        if (beanClass.isAnnotationPresent(PulsarProducer.class) && bean instanceof PulsarProducerFactory) {
            producers.putAll(((PulsarProducerFactory) bean).getTopics().entrySet().stream()
                    .map($ -> $.getValue().right.map(cluster -> new ProducerHolder(
                            stringValueResolver.resolveStringValue($.getKey()),
                            $.getValue().left,
                            $.getValue().middle,
                            cluster)
                    ).orElseGet(() -> new ProducerHolder(
                            stringValueResolver.resolveStringValue($.getKey()),
                            $.getValue().left,
                            $.getValue().middle)
                    ))
                    .collect(Collectors.toMap(ProducerHolder::getTopic, this::buildProducer)));
        }

        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        return bean;
    }

    private Producer<?> buildProducer(ProducerHolder holder) {
        try {
            String cluster = holder.getCluster().orElse(PulsarClientContainer.DEFAULT_CLUSTER);
            final ProducerBuilder<?> producerBuilder = clientContainer.getClient(cluster)
                    .newProducer(getSchema(holder))
                    .topic(urlBuildService.buildTopicUrl(holder.getTopic()));

            if (clientContainer.getProperties(cluster).isAllowInterceptor()) {
                producerBuilder.intercept(producerInterceptor);
            }

            return producerBuilder.create();
        } catch (PulsarClientException e) {
            throw new ProducerInitException("Failed to init producer.", e);
        }
    }

    private <T> Schema<?> getSchema(ProducerHolder holder) throws RuntimeException {
        return SchemaUtils.getSchema(holder.getSerialization(), holder.getClazz());
    }

    public Producer getProducer(String topic) {
        return producers.get(stringValueResolver.resolveStringValue(topic));
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver stringValueResolver) {
        this.stringValueResolver = stringValueResolver;
    }
}
