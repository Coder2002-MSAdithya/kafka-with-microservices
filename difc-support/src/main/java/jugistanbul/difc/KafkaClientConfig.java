package jugistanbul.difc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/** Shared Kafka client properties for the JUG Istanbul DIFC demo. */
public final class KafkaClientConfig {

    private KafkaClientConfig() {
    }

    public static Properties producerProps(
            final String principal,
            final String password,
            final String keySerializer,
            final String valueSerializer) {
        final Properties props = baseProducerProps(principal, password, keySerializer, valueSerializer);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, principal + "-producer");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, principal + "-" + UUID.randomUUID());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return props;
    }

    /** Non-transactional producer for tag ownership and {@code POLL_PRIVS_REQ} grantor polling. */
    public static Properties grantorProducerProps(
            final String principal,
            final String password,
            final String keySerializer,
            final String valueSerializer) {
        final Properties props = baseProducerProps(principal, password, keySerializer, valueSerializer);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, principal + "-grantor");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        return props;
    }

    private static Properties baseProducerProps(
            final String principal,
            final String password,
            final String keySerializer,
            final String valueSerializer) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DifcConstants.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        applySasl(props, principal, password);
        return props;
    }

    @SuppressWarnings("unchecked")
    private static void applySasl(final Properties props, final String principal, final String password) {
        DifcKafkaProperties.applySasl((Map<String, Object>) (Map<?, ?>) props, principal, password);
    }

    public static Properties consumerProps(
            final String principal,
            final String password,
            final String groupId,
            final String keyDeserializer,
            final String valueDeserializer) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DifcConstants.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, principal + "-consumer");
        applySasl(props, principal, password);
        return props;
    }

    /** Consumer props with tolerant {@code EventObjectDeserializer} (null on bad payloads). */
    public static Properties consumerPropsForEventObject(
            final String principal,
            final String password,
            final String groupId) {
        return consumerProps(
                principal,
                password,
                groupId,
                "org.apache.kafka.common.serialization.IntegerDeserializer",
                "jugistanbul.deserializer.EventObjectDeserializer");
    }

    public static String passwordFor(final String principal) {
        return System.getenv().getOrDefault(
                principal.toUpperCase().replace('-', '_') + "_PASSWORD",
                principal + "-secret");
    }

    public static boolean difcEnabled() {
        final String sys = System.getProperty("DIFC_ENABLED");
        if (sys != null) {
            return !"false".equalsIgnoreCase(sys);
        }
        return !"false".equalsIgnoreCase(System.getenv().getOrDefault("DIFC_ENABLED", "true"));
    }

    public static void putIfPresent(final Properties props, final Map<String, String> env, final String key) {
        final String value = env.get(key);
        if (value != null && !value.isEmpty()) {
            props.put(key, value);
        }
    }
}
