package io.github.majusko.pulsar;

import io.github.majusko.pulsar.consumer.DefaultConsumerInterceptor;
import io.github.majusko.pulsar.producer.DefaultProducerInterceptor;
import io.github.majusko.pulsar.properties.ConsumerProperties;
import io.github.majusko.pulsar.properties.PulsarProperties;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.handler.IgnoreErrorsBindHandler;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.boot.validation.MessageInterpolatorFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

/**
 * Pulsar auto configuration for Spring Boot
 *
 * @author MiNG
 * @since 1.0.0
 **/
@Configuration
@EnableConfigurationProperties({PulsarProperties.class, ConsumerProperties.class})
public class PulsarAutoConfiguration implements ApplicationContextAware, EnvironmentAware, InitializingBean {

    private Map<String, PulsarProperties> pulsarProperties;
    private Environment environment;
    private ApplicationContext applicationContext;


    @Bean
    PulsarClientContainer pulsarClientContainer() {
        return new DefaultPulsarClientContainer(this.pulsarProperties);
    }

    @Override
    public void afterPropertiesSet() {
        this.pulsarProperties = this.bindPulsarProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public ProducerInterceptor producerInterceptor(){
        return new DefaultProducerInterceptor();
    }

    @Bean
    @ConditionalOnMissingBean
    public ConsumerInterceptor consumerInterceptor(){
        return new DefaultConsumerInterceptor();
    }


    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void setEnvironment(final Environment environment) {
        this.environment = environment;
    }

    private Map<String, PulsarProperties> bindPulsarProperties() {
        // Changes since 1.3.0
        // ===================
        //
        // Since 1.3.0, we change binding procedure from
        //   find keys -> bind each "pulsar.*" to PulsarProperties
        // to
        //   bind to Map<String, PulsarProperties> directly
        //
        // After some tests, we found that when "pulsar.*" (direct level property of "pulsar"):
        // - is value, and no converter found, throws BindException but will be suppressed by IgnoreErrorsBindHandler
        // - is array, type mismatch, returns null
        // - is nested, but none properties match (name and type) to PulsarProperties, returns null
        // - is nested, and some properties match to PulsarProperties, consider as the modern style configuration, validation will run
        return Binder.get(this.environment)
                .bind("pulsar", Bindable.mapOf(String.class, PulsarProperties.class), this.getBindHandler())
                .orElse(emptyMap())
                .entrySet()
                .stream()
                .filter(it -> it.getValue() != null)
                .collect(Collectors.toMap(it -> it.getKey(), it -> it.getValue()));
    }

    private BindHandler getBindHandler() {

        // inspired by org.springframework.boot.context.properties.ConfigurationPropertiesBinder.getBindHandler

        BindHandler bindHandler;
        // @ConfigurationProperties(ignoreInvalidFields = true)
        bindHandler = new IgnoreErrorsBindHandler();
        // @Validated
        bindHandler = this.getValidationBindHandler(bindHandler);
        return bindHandler;
    }

    private <T> BindHandler getValidationBindHandler(final BindHandler bindHandler) {

        // inspired by org.springframework.boot.context.properties.ConfigurationPropertiesJsr303Validator

        final LocalValidatorFactoryBean localValidatorFactoryBean = new LocalValidatorFactoryBean();
        {
            localValidatorFactoryBean.setApplicationContext(this.applicationContext);
            localValidatorFactoryBean.setMessageInterpolator(new MessageInterpolatorFactory().getObject());
            localValidatorFactoryBean.afterPropertiesSet();
        }

        return new ValidationBindHandler(bindHandler, new Validator() {
            @Override
            public boolean supports(final Class<?> clazz) {
                return localValidatorFactoryBean.supports(clazz);
            }

            @Override
            public void validate(final Object target, final Errors errors) {
                localValidatorFactoryBean.validate(target, errors);
            }
        });
    }
}
