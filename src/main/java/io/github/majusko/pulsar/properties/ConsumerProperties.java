package io.github.majusko.pulsar.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConstructorBinding;

@Data
@ConstructorBinding
public class ConsumerProperties {
    int deadLetterPolicyMaxRedeliverCount = -1;
    int ackTimeoutMs = 0;
    String subscriptionType = "";

    public ConsumerProperties(int deadLetterPolicyMaxRedeliverCount, int ackTimeoutMs, String subscriptionType) {
        this.deadLetterPolicyMaxRedeliverCount = deadLetterPolicyMaxRedeliverCount;
        this.ackTimeoutMs = ackTimeoutMs;
        this.subscriptionType = subscriptionType;
    }
}