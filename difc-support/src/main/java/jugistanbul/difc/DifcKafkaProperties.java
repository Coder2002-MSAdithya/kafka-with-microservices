package jugistanbul.difc;

import java.util.Map;

/** Shared SASL/SCRAM client properties for DIFC microservices. */
public final class DifcKafkaProperties {

    private DifcKafkaProperties() {
    }

    public static void applySasl(final Map<String, Object> props, final String principal, final String password) {
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put(
                "sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"" + principal + "\" password=\"" + password + "\";");
    }
}
