package jugistanbul.stockservice;

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
import java.util.HashSet;
import java.util.Set;

public final class StockService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StockService.class);

    private StockService() {
    }

    public static void main(final String[] args) {
        final String password = KafkaClientConfig.passwordFor(DifcConstants.PRINCIPAL_STOCK);
        try (DifcGrantorBootstrap grantor = DifcGrantorBootstrap.start(
                     LOGGER,
                     DifcConstants.SERVICE_STOCK,
                     DifcConstants.PRINCIPAL_STOCK,
                     password,
                     DifcConstants.TAG_STOCK);
             KafkaConsumer<Integer, EventObject> consumer = new KafkaConsumer<>(
                KafkaClientConfig.consumerPropsForEventObject(
                        DifcConstants.PRINCIPAL_STOCK,
                        password,
                        "orderEventConsumerGroup01"));
             KafkaProducer<Integer, EventObject> producer = new KafkaProducer<>(
                     KafkaClientConfig.producerProps(
                             DifcConstants.PRINCIPAL_STOCK,
                             password,
                             IntegerSerializer.class.getName(),
                             EventObjectSerializer.class.getName()))) {

            bootstrapDifcRegister(consumer, producer);
            consumer.subscribe(Set.of(DifcConstants.TOPIC_ORDER));
            producer.initTransactions();
            bootstrapDifcComplete(consumer);
            LOGGER.info("Stock service listening on {}", DifcConstants.TOPIC_ORDER);

            while (true) {
                final ConsumerRecords<Integer, EventObject> records = consumer.poll(Duration.ofMillis(200));
                for (final ConsumerRecord<Integer, EventObject> record : records) {
                    final EventObject in = record.value();
                    if (in == null) {
                        LOGGER.warn(
                                "Skipping null order event partition={} offset={}",
                                record.partition(),
                                record.offset());
                        continue;
                    }
                    LOGGER.info(
                            "Consumed order key={} product={} amount={}",
                            record.key(),
                            in.getProductId(),
                            in.getAmount());
                    final boolean inStock = in.getProductId() != null && in.getProductId() > 0
                            && in.getAmount() != null && in.getAmount() > 0;
                    publishStockCheck(producer, in, inStock);
                }
                consumer.commitSync();
            }
        } catch (final Exception ex) {
            LOGGER.error("Stock service failed", ex);
            System.exit(1);
        }
    }

    private static void bootstrapDifcRegister(
            final KafkaConsumer<Integer, EventObject> consumer,
            final KafkaProducer<Integer, EventObject> producer) {
        if (!KafkaClientConfig.difcEnabled()) {
            return;
        }
        DifcRequester.registerClient(LOGGER, DifcConstants.SERVICE_STOCK, consumer);
        DifcRequester.registerClient(LOGGER, DifcConstants.SERVICE_STOCK, producer);
        DifcRequester.requestGrantCapAdd(
                LOGGER, DifcConstants.SERVICE_STOCK, consumer, DifcConstants.TAG_ORDER);
        DifcRequester.requestGrantCapForConsume(
                LOGGER, DifcConstants.SERVICE_STOCK, consumer, DifcConstants.TAG_CARD);
    }

    private static void bootstrapDifcComplete(final KafkaConsumer<Integer, EventObject> consumer) {
        if (!KafkaClientConfig.difcEnabled()) {
            return;
        }
        DifcRequester.completeCanRemoveGrant(
                LOGGER, DifcConstants.SERVICE_STOCK, consumer, DifcConstants.TAG_ORDER);
        LOGGER.info("[StockService] DIFC grants ready");
        System.out.println("[StockService] DIFC grants ready");
    }

    private static void publishStockCheck(
            final KafkaProducer<Integer, EventObject> producer,
            final EventObject in,
            final boolean inStock) throws Exception {
        final EventObject out = EventProjections.forStockCheck(in, inStock);
        final ProducerRecord<Integer, EventObject> record =
                new ProducerRecord<>(DifcConstants.TOPIC_STOCK_CHECK, out.getCustomerId(), out);
        producer.beginTransaction();
        if (KafkaClientConfig.difcEnabled()) {
            final Set<String> attach = new HashSet<>();
            attach.add(DifcConstants.TAG_STOCK);
            if (out.getCardNumber() != null && !out.getCardNumber().isEmpty()) {
                attach.add(DifcConstants.TAG_CARD);
            }
            producer.sendWithTags(
                    record.declassifyTags(Set.of(DifcConstants.TAG_ORDER)),
                    attach,
                    null).get();
        } else {
            producer.send(record).get();
        }
        producer.commitTransaction();
        LOGGER.info("Published stock-check customer={} inStock={}", out.getCustomerId(), inStock);
    }
}
