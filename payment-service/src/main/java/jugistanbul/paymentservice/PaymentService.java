package jugistanbul.paymentservice;

import jugistanbul.difc.DifcConstants;
import jugistanbul.difc.DifcGrantorBootstrap;
import jugistanbul.difc.DifcRequester;
import jugistanbul.difc.KafkaClientConfig;
import jugistanbul.entity.EventObject;
import jugistanbul.entity.EventProjections;
import jugistanbul.serializer.EventObjectSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public final class PaymentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaymentService.class);

    private PaymentService() {
    }

    public static void main(final String[] args) {
        final String password = KafkaClientConfig.passwordFor(DifcConstants.PRINCIPAL_PAYMENT);
        try (DifcGrantorBootstrap grantor = DifcGrantorBootstrap.start(
                     LOGGER,
                     DifcConstants.SERVICE_PAYMENT,
                     DifcConstants.PRINCIPAL_PAYMENT,
                     password,
                     DifcConstants.TAG_BILLING);
             KafkaConsumer<Integer, EventObject> consumer = new KafkaConsumer<>(
                KafkaClientConfig.consumerPropsForEventObject(
                        DifcConstants.PRINCIPAL_PAYMENT,
                        password,
                        "validationEventConsumerGroup02"));
             KafkaProducer<Integer, EventObject> producer = new KafkaProducer<>(
                     KafkaClientConfig.producerProps(
                             DifcConstants.PRINCIPAL_PAYMENT,
                             password,
                             IntegerSerializer.class.getName(),
                             EventObjectSerializer.class.getName()))) {

            bootstrapDifcRegister(consumer, producer);
            consumer.subscribe(Set.of(DifcConstants.TOPIC_VALIDATION));
            producer.initTransactions();
            bootstrapDifcComplete(consumer);
            LOGGER.info("Payment service listening on {}", DifcConstants.TOPIC_VALIDATION);

            while (true) {
                final ConsumerRecords<Integer, EventObject> records = consumer.poll(Duration.ofMillis(200));
                for (final ConsumerRecord<Integer, EventObject> record : records) {
                    final EventObject in = record.value();
                    if (in == null) {
                        LOGGER.warn(
                                "Skipping null/undeserializable event partition={} offset={}",
                                record.partition(),
                                record.offset());
                        continue;
                    }
                    if (!in.isNumberValid()) {
                        continue;
                    }
                    final int billedPrice = ThreadLocalRandom.current().nextInt(100, 500);
                    publishBilling(producer, in, billedPrice);
                }
                consumer.commitSync();
            }
        } catch (final Exception ex) {
            LOGGER.error("Payment service failed", ex);
            System.exit(1);
        }
    }

    private static void bootstrapDifcRegister(
            final KafkaConsumer<Integer, EventObject> consumer,
            final KafkaProducer<Integer, EventObject> producer) {
        if (!KafkaClientConfig.difcEnabled()) {
            return;
        }
        DifcRequester.registerClient(LOGGER, DifcConstants.SERVICE_PAYMENT, consumer);
        DifcRequester.registerClient(LOGGER, DifcConstants.SERVICE_PAYMENT, producer);
        DifcRequester.requestGrantCapAdd(
                LOGGER, DifcConstants.SERVICE_PAYMENT, consumer, DifcConstants.TAG_VALIDATION);
    }

    private static void bootstrapDifcComplete(final KafkaConsumer<Integer, EventObject> consumer) {
        if (!KafkaClientConfig.difcEnabled()) {
            return;
        }
        DifcRequester.completeCanRemoveGrant(
                LOGGER, DifcConstants.SERVICE_PAYMENT, consumer, DifcConstants.TAG_VALIDATION);
        LOGGER.info("[PaymentService] DIFC grants ready");
        System.out.println("[PaymentService] DIFC grants ready");
    }

    private static void publishBilling(
            final KafkaProducer<Integer, EventObject> producer,
            final EventObject in,
            final int price) throws Exception {
        final EventObject out = EventProjections.forBilling(in, price);
        final ProducerRecord<Integer, EventObject> record =
                new ProducerRecord<>(DifcConstants.TOPIC_BILLING, out.getCustomerId(), out);
        producer.beginTransaction();
        if (KafkaClientConfig.difcEnabled()) {
            producer.sendWithTags(
                    record.declassifyTags(Set.of(DifcConstants.TAG_VALIDATION)),
                    Set.of(DifcConstants.TAG_BILLING),
                    null).get();
        } else {
            producer.send(record).get();
        }
        producer.commitTransaction();
        LOGGER.info("Published billing customer={} price={}", out.getCustomerId(), price);
    }
}
