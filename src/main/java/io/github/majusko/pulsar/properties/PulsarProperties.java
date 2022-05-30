package io.github.majusko.pulsar.properties;

import lombok.Getter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConstructorBinding;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.HashSet;
import java.util.Set;

@Getter
@ToString
@ConstructorBinding
public class PulsarProperties {
    private String serviceUrl = "pulsar://localhost:6650";
    private Integer ioThreads = 10;
    private Integer listenerThreads = 10;
    private boolean enableTcpNoDelay = false;
    private Integer keepAliveIntervalSec = 20;
    private Integer connectionTimeoutSec = 10;
    private Integer operationTimeoutSec = 15;
    private Integer startingBackoffIntervalMs = 100;
    private Integer maxBackoffIntervalSec = 10;
    private String consumerNameDelimiter = "";
    private String namespace = "default";
    private String tenant = "public";
    private String tlsTrustCertsFilePath = null;
    private Set<String> tlsCiphers = new HashSet<>();
    private Set<String> tlsProtocols = new HashSet<>();
    private String tlsTrustStorePassword = null;
    private String tlsTrustStorePath = null;
    private String tlsTrustStoreType = null;
    private boolean useKeyStoreTls = false;
    private boolean allowTlsInsecureConnection = false;
    private boolean enableTlsHostnameVerification = false;
    private String tlsAuthCertFilePath = null;
    private String tlsAuthKeyFilePath = null;
    private String tokenAuthValue = null;
    private String oauth2IssuerUrl = null;
    private String oauth2CredentialsUrl = null;
    private String oauth2Audience = null;
    private boolean autoStart = true;
    private boolean allowInterceptor = false;
    private String listenerName = null;

    /**
     * 消费者配置。
     */
    @NestedConfigurationProperty
    private final ConsumerProperties consumer;

    public PulsarProperties(String serviceUrl,
                            @DefaultValue("10") Integer ioThreads,
                            @DefaultValue("10") Integer listenerThreads,
                            boolean enableTcpNoDelay,
                            Integer keepAliveIntervalSec,
                            Integer connectionTimeoutSec,
                            Integer operationTimeoutSec,
                            Integer startingBackoffIntervalMs,
                            Integer maxBackoffIntervalSec,
                            String consumerNameDelimiter,
                            String namespace,
                            String tenant,
                            String tlsTrustCertsFilePath,
                            Set<String> tlsCiphers, Set<String> tlsProtocols,
                            String tlsTrustStorePassword, String tlsTrustStorePath,
                            String tlsTrustStoreType,
                            boolean useKeyStoreTls,
                            boolean allowTlsInsecureConnection,
                            boolean enableTlsHostnameVerification,
                            String tlsAuthCertFilePath,
                            String tlsAuthKeyFilePath,
                            String tokenAuthValue,
                            String oauth2IssuerUrl,
                            String oauth2CredentialsUrl,
                            String oauth2Audience,
                            boolean autoStart,
                            boolean allowInterceptor,
                            String listenerName,
                            ConsumerProperties consumer) {

        this.serviceUrl = serviceUrl;
        this.ioThreads = ioThreads;
        this.listenerThreads = listenerThreads;
        this.enableTcpNoDelay = enableTcpNoDelay;
        this.keepAliveIntervalSec = keepAliveIntervalSec;
        this.connectionTimeoutSec = connectionTimeoutSec;
        this.operationTimeoutSec = operationTimeoutSec;
        this.startingBackoffIntervalMs = startingBackoffIntervalMs;
        this.maxBackoffIntervalSec = maxBackoffIntervalSec;
        this.consumerNameDelimiter = consumerNameDelimiter;
        this.namespace = namespace;
        this.tenant = tenant;
        this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
        this.tlsCiphers = tlsCiphers;
        this.tlsProtocols = tlsProtocols;
        this.tlsTrustStorePassword = tlsTrustStorePassword;
        this.tlsTrustStorePath = tlsTrustStorePath;
        this.tlsTrustStoreType = tlsTrustStoreType;
        this.useKeyStoreTls = useKeyStoreTls;
        this.allowTlsInsecureConnection = allowTlsInsecureConnection;
        this.enableTlsHostnameVerification = enableTlsHostnameVerification;
        this.tlsAuthCertFilePath = tlsAuthCertFilePath;
        this.tlsAuthKeyFilePath = tlsAuthKeyFilePath;
        this.tokenAuthValue = tokenAuthValue;
        this.oauth2IssuerUrl = oauth2IssuerUrl;
        this.oauth2CredentialsUrl = oauth2CredentialsUrl;
        this.oauth2Audience = oauth2Audience;
        this.autoStart = autoStart;
        this.allowInterceptor = allowInterceptor;
        this.listenerName = listenerName;
        this.consumer = consumer;
    }
}