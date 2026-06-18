package jugistanbul.orderservice.kafka.consumer;

import jugistanbul.difc.DifcConstants;
import jugistanbul.difc.DifcRequester;
import jugistanbul.difc.KafkaClientConfig;
import jugistanbul.entity.EventObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;

public class EventConsumer implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);

    private final KafkaConsumer<Integer, EventObject> consumer;
    private final java.util.function.Consumer<EventObject> handler;
    private final String principal;
    private final String serviceName;
    private final String grantTag;
    private final String[] topics;
    private volatile boolean running = true;

    public EventConsumer(
            final String principal,
            final String serviceName,
            final String groupId,
            final String grantTag,
            final java.util.function.Consumer<EventObject> handler,
            final String... topics) {
        this.principal = principal;
        this.serviceName = serviceName;
        this.grantTag = grantTag;
        this.handler = handler;
        this.topics = topics;
        final String password = KafkaClientConfig.passwordFor(principal);
        this.consumer = new KafkaConsumer<>(
                KafkaClientConfig.consumerPropsForEventObject(principal, password, groupId));
    }

    @Override
    public void run() {
        try {
            if (KafkaClientConfig.difcEnabled() && grantTag != null) {
                DifcRequester.registerClient(LOGGER, serviceName, consumer);
                DifcRequester.requestGrantCapForConsume(LOGGER, serviceName, consumer, grantTag);
            }
            consumer.subscribe(Arrays.asList(topics));
            LOGGER.info("Consumer subscribed principal={} tag={} topics={}", principal, grantTag, Arrays.toString(topics));
            while (running) {
                final ConsumerRecords<Integer, EventObject> records = consumer.poll(Duration.ofMillis(200));
                for (final ConsumerRecord<Integer, EventObject> record : records) {
                    final EventObject value = record.value();
                    if (value == null) {
                        LOGGER.warn(
                                "Skipping null/undeserializable event topic={} partition={} offset={}",
                                record.topic(),
                                record.partition(),
                                record.offset());
                        continue;
                    }
                    handler.accept(value);
                }
                consumer.commitSync();
            }
        } catch (final Exception ex) {
            LOGGER.error("Consumer failed principal={} tag={}", principal, grantTag, ex);
        } finally {
            consumer.close();
        }
    }

    public void stop() {
        running = false;
    }
}
