package jugistanbul.difc;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;

import java.util.Properties;

/**
 * Dedicated non-transactional producer that owns DIFC tags and polls {@code POLL_PRIVS_REQ}.
 */
public final class DifcGrantorBootstrap implements AutoCloseable {

    private final KafkaProducer<Integer, byte[]> producer;
    private final DifcPrivilegePoller poller;

    private DifcGrantorBootstrap(
            final KafkaProducer<Integer, byte[]> producer,
            final DifcPrivilegePoller poller) {
        this.producer = producer;
        this.poller = poller;
    }

    public static DifcGrantorBootstrap start(
            final Logger log,
            final String serviceName,
            final String principal,
            final String password,
            final String... ownedTags) {
        if (!KafkaClientConfig.difcEnabled()) {
            return new DifcGrantorBootstrap(null, null);
        }
        final Properties props = KafkaClientConfig.grantorProducerProps(
                principal,
                password,
                IntegerSerializer.class.getName(),
                ByteArraySerializer.class.getName());
        final KafkaProducer<Integer, byte[]> grantorProducer = new KafkaProducer<>(props);
        DifcRequester.registerClient(log, serviceName, grantorProducer);
        for (final String tag : ownedTags) {
            DifcRequester.createTag(log, serviceName, grantorProducer, tag);
        }
        final DifcPrivilegePoller privilegePoller = new DifcPrivilegePoller(
                log,
                serviceName,
                grantorProducer,
                DifcGrantor.autoGrantForProducer(log, serviceName, grantorProducer));
        privilegePoller.start();
        final String tagList = String.join(",", ownedTags);
        log.info("[{}] DIFC grantor ready (tags={})", serviceName, tagList);
        System.out.printf("[%s] DIFC grantor ready (tags=%s)%n", serviceName, tagList);
        return new DifcGrantorBootstrap(grantorProducer, privilegePoller);
    }

    @Override
    public void close() {
        if (poller != null) {
            poller.close();
        }
        if (producer != null) {
            producer.close();
        }
    }
}
