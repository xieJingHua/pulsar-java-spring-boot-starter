package io.github.majusko.pulsar;


import com.google.common.base.Strings;
import io.github.majusko.pulsar.error.exception.ClientInitException;
import io.github.majusko.pulsar.properties.PulsarProperties;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import java.net.URL;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author MiNG
 * @since 1.0.0
 */
public class DefaultPulsarClientContainer implements PulsarClientContainer, AutoCloseable {

    private final Map<String/*cluster*/, PulsarClient> clients;

    private final Map<String/*cluster*/, PulsarProperties> properties;

    public DefaultPulsarClientContainer(final Map<String, PulsarProperties> properties){
        this.properties = properties;
        this.clients = properties.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        kv -> kv.getKey(),
                        kv -> buildPulsarClient(kv.getValue())
                ));
    }


    @SneakyThrows
    public static PulsarClient buildPulsarClient(final PulsarProperties pulsarProperties) {
        if (!Strings.isNullOrEmpty(pulsarProperties.getTlsAuthCertFilePath()) &&
                !Strings.isNullOrEmpty(pulsarProperties.getTlsAuthKeyFilePath()) &&
                !Strings.isNullOrEmpty(pulsarProperties.getTokenAuthValue())
        ) throw new ClientInitException("You cannot use multiple auth options.");

        final ClientBuilder pulsarClientBuilder = PulsarClient.builder()
                .serviceUrl(pulsarProperties.getServiceUrl())
                .ioThreads(pulsarProperties.getIoThreads())
                .listenerThreads(pulsarProperties.getListenerThreads())
                .enableTcpNoDelay(pulsarProperties.isEnableTcpNoDelay())
                .keepAliveInterval(pulsarProperties.getKeepAliveIntervalSec(), TimeUnit.SECONDS)
                .connectionTimeout(pulsarProperties.getConnectionTimeoutSec(), TimeUnit.SECONDS)
                .operationTimeout(pulsarProperties.getOperationTimeoutSec(), TimeUnit.SECONDS)
                .startingBackoffInterval(pulsarProperties.getStartingBackoffIntervalMs(), TimeUnit.MILLISECONDS)
                .maxBackoffInterval(pulsarProperties.getMaxBackoffIntervalSec(), TimeUnit.SECONDS)
                .useKeyStoreTls(pulsarProperties.isUseKeyStoreTls())
                .tlsTrustCertsFilePath(pulsarProperties.getTlsTrustCertsFilePath())
                .tlsCiphers(pulsarProperties.getTlsCiphers())
                .tlsProtocols(pulsarProperties.getTlsProtocols())
                .tlsTrustStorePassword(pulsarProperties.getTlsTrustStorePassword())
                .tlsTrustStorePath(pulsarProperties.getTlsTrustStorePath())
                .tlsTrustStoreType(pulsarProperties.getTlsTrustStoreType())
                .allowTlsInsecureConnection(pulsarProperties.isAllowTlsInsecureConnection())
                .enableTlsHostnameVerification(pulsarProperties.isEnableTlsHostnameVerification());

        if (!Strings.isNullOrEmpty(pulsarProperties.getTlsAuthCertFilePath()) &&
                !Strings.isNullOrEmpty(pulsarProperties.getTlsAuthKeyFilePath())) {
            pulsarClientBuilder.authentication(AuthenticationFactory
                    .TLS(pulsarProperties.getTlsAuthCertFilePath(), pulsarProperties.getTlsAuthKeyFilePath()));
        }

        if (!Strings.isNullOrEmpty(pulsarProperties.getTokenAuthValue())) {
            pulsarClientBuilder.authentication(AuthenticationFactory
                    .token(pulsarProperties.getTokenAuthValue()));
        }

        if (!Strings.isNullOrEmpty(pulsarProperties.getOauth2Audience()) &&
                !Strings.isNullOrEmpty(pulsarProperties.getOauth2IssuerUrl()) &&
                !Strings.isNullOrEmpty(pulsarProperties.getOauth2CredentialsUrl())) {
            final URL issuerUrl = new URL(pulsarProperties.getOauth2IssuerUrl());
            final URL credentialsUrl = new URL(pulsarProperties.getOauth2CredentialsUrl());

            pulsarClientBuilder.authentication(AuthenticationFactoryOAuth2
                    .clientCredentials(issuerUrl, credentialsUrl, pulsarProperties.getOauth2Audience()));
        }
        if (!Strings.isNullOrEmpty(pulsarProperties.getListenerName())) {
            pulsarClientBuilder.listenerName(pulsarProperties.getListenerName());
        }

        return pulsarClientBuilder.build();
    }


    @Override
    public PulsarClient getClient(final String cluster) {
        return Objects.requireNonNull(this.clients.get(cluster), "cluster [" + cluster + "] is not configured");
    }


    @Override
    public PulsarClient findClient(final String cluster) {
        return this.clients.get(cluster);
    }


    @Override
    public PulsarProperties getProperties(final String cluster) {
        return Objects.requireNonNull(this.properties.get(cluster), "cluster [" + cluster + "] is not configured");
    }



    @Override
    public PulsarProperties findProperties(final String cluster) {
        return this.properties.get(cluster);
    }

    public Map<String, PulsarProperties> getAllProperties() {
        return properties;
    }

    @Override
    public void close() {
        final IllegalStateException exception = new IllegalStateException("Errors occurred while closing " + this.getClass().getName());
        for (final PulsarClient client : this.clients.values()) {
            try {
                client.close();
            } catch (final Exception e) {
                exception.addSuppressed(e);
            }
        }
        if (ArrayUtils.isNotEmpty(exception.getSuppressed())) {
            throw exception;
        }
    }
}
