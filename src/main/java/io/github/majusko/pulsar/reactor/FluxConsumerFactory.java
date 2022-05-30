package io.github.majusko.pulsar.reactor;

import io.github.majusko.pulsar.PulsarClientContainer;
import io.github.majusko.pulsar.error.exception.ClientInitException;
import io.github.majusko.pulsar.properties.ConsumerProperties;
import io.github.majusko.pulsar.properties.PulsarProperties;
import io.github.majusko.pulsar.utils.SchemaUtils;
import io.github.majusko.pulsar.utils.UrlBuildService;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.*;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class FluxConsumerFactory {
    private final UrlBuildService urlBuildService;
    private final ConsumerInterceptor consumerInterceptor;
    private final PulsarClientContainer clientContainer;

    private List<Consumer> consumers = new ArrayList<>();

    public FluxConsumerFactory(PulsarClientContainer clientContainer, UrlBuildService urlBuildService, ConsumerInterceptor consumerInterceptor) {
        this.clientContainer = clientContainer;
        this.urlBuildService = urlBuildService;
        this.consumerInterceptor = consumerInterceptor;
    }

    public <T> FluxConsumer<T> newConsumer(PulsarFluxConsumer<T> fluxConsumer) throws ClientInitException, PulsarClientException {
        final SubscriptionType subscriptionType = urlBuildService.getSubscriptionType(fluxConsumer.getSubscriptionType());
        String cluster = StringUtils.isNotBlank(fluxConsumer.getCluster()) ? fluxConsumer.getCluster() : PulsarClientContainer.DEFAULT_CLUSTER;
        final ConsumerBuilder<?> consumerBuilder = clientContainer.getClient(cluster)
            .newConsumer(SchemaUtils.getSchema(fluxConsumer.getSerialization(), fluxConsumer.getMessageClass()))
            .consumerName(fluxConsumer.getConsumerName())
            .subscriptionName(fluxConsumer.getSubscriptionName())
            .topic(urlBuildService.buildTopicUrl(fluxConsumer.getTopic(), fluxConsumer.getNamespace()))
            .subscriptionInitialPosition(fluxConsumer.getInitialPosition())
            .subscriptionType(subscriptionType)
            .messageListener((consumer, msg) -> {
                try {
                    if(fluxConsumer.isSimple()) {
                        fluxConsumer.simpleEmit((T) msg.getValue());
                        consumer.acknowledge(msg);
                    } else {
                        fluxConsumer.emit(new FluxConsumerHolder(consumer, msg));
                    }
                } catch (Exception e) {
                    consumer.negativeAcknowledge(msg);

                    if(fluxConsumer.isSimple()) {
                        fluxConsumer.simpleEmitError(e);
                    } else {
                        fluxConsumer.emitError(e);
                    }
                }
            });

        if(clientContainer.getProperties(cluster).isAllowInterceptor()) {
            consumerBuilder.intercept(consumerInterceptor);
        }

        ConsumerProperties consumerProperties = clientContainer.getProperties(cluster).getConsumer();
        if (consumerProperties!=null && consumerProperties.getAckTimeoutMs() > 0) {
            consumerBuilder.ackTimeout(consumerProperties.getAckTimeoutMs(), TimeUnit.MILLISECONDS);
        }

        urlBuildService.buildDeadLetterPolicy(fluxConsumer.getMaxRedeliverCount(), fluxConsumer.getDeadLetterTopic(), consumerBuilder);

        consumers.add(consumerBuilder.subscribe());

        return fluxConsumer;
    }

    public List<Consumer> getConsumers() {
        return consumers;
    }
}
