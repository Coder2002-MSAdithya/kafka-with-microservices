package jugistanbul.orderservice.kafka.event.stockcheck.producer;

import jugistanbul.difc.DifcConstants;
import jugistanbul.difc.DifcGrantorBootstrap;
import jugistanbul.difc.KafkaClientConfig;
import jugistanbul.entity.EventObject;
import jugistanbul.serializer.EventObjectSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Startup
@Singleton
public class OrderEventProducer {

    private static final int INIT_MAX_ATTEMPTS = 90;
    private static final long INIT_RETRY_MS = 2000L;

    private final AtomicReference<KafkaProducer<Integer, EventObject>> producer =
            new AtomicReference<>();
    private volatile DifcGrantorBootstrap grantorBootstrap;
    private final CountDownLatch readyLatch = new CountDownLatch(1);
    private volatile String initFailure;
    private ExecutorService initExecutor;

    @Inject
    private Logger logger;

    public OrderEventProducer() {
    }

    @PostConstruct
    public void initProducer() {
        logger.info("Scheduling asynchronous order event producer initialization...");
        initExecutor =
                Executors.newSingleThreadExecutor(
                        r -> {
                            final Thread thread = new Thread(r, "order-kafka-init");
                            thread.setDaemon(true);
                            return thread;
                        });
        initExecutor.execute(this::initializeWithRetry);
    }

    private void initializeWithRetry() {
        final String password = KafkaClientConfig.passwordFor(DifcConstants.PRINCIPAL_ORDER);
        try {
            for (int attempt = 1; attempt <= INIT_MAX_ATTEMPTS; attempt++) {
                try {
                    if (KafkaClientConfig.difcEnabled()) {
                        grantorBootstrap =
                                DifcGrantorBootstrap.start(
                                        logger,
                                        DifcConstants.SERVICE_ORDER,
                                        DifcConstants.PRINCIPAL_ORDER,
                                        password,
                                        DifcConstants.TAG_ORDER,
                                        DifcConstants.TAG_CARD);
                    }
                    final Properties props =
                            KafkaClientConfig.producerProps(
                                    DifcConstants.PRINCIPAL_ORDER,
                                    password,
                                    IntegerSerializer.class.getName(),
                                    EventObjectSerializer.class.getName());
                    final KafkaProducer<Integer, EventObject> created = new KafkaProducer<>(props);
                    created.initTransactions();
                    producer.set(created);
                    initFailure = null;
                    logger.info(
                            "Order event producer initialized (attempt {}/{})",
                            attempt,
                            INIT_MAX_ATTEMPTS);
                    return;
                } catch (final Exception ex) {
                    shutdownPartial();
                    initFailure = ex.getMessage();
                    logger.warn(
                            "Order event producer init attempt {}/{} failed: {}",
                            attempt,
                            INIT_MAX_ATTEMPTS,
                            ex.toString());
                    sleepQuietly(INIT_RETRY_MS);
                }
            }
            logger.error("Order event producer failed to initialize after {} attempts", INIT_MAX_ATTEMPTS);
        } finally {
            readyLatch.countDown();
        }
    }

    private void shutdownPartial() {
        if (grantorBootstrap != null) {
            grantorBootstrap.close();
            grantorBootstrap = null;
        }
        final KafkaProducer<Integer, EventObject> existing = producer.getAndSet(null);
        if (existing != null) {
            existing.close();
        }
    }

    private static void sleepQuietly(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void shutdown() {
        if (initExecutor != null) {
            initExecutor.shutdownNow();
        }
        if (grantorBootstrap != null) {
            grantorBootstrap.close();
        }
        final KafkaProducer<Integer, EventObject> existing = producer.getAndSet(null);
        if (existing != null) {
            existing.close();
        }
    }

    @Produces
    public KafkaProducer<Integer, EventObject> getProducer() {
        awaitReady();
        final KafkaProducer<Integer, EventObject> ready = producer.get();
        if (ready == null) {
            throw new IllegalStateException(
                    "Order Kafka producer is not ready"
                            + (initFailure == null ? "" : ": " + initFailure));
        }
        return ready;
    }

    private void awaitReady() {
        if (producer.get() != null) {
            return;
        }
        try {
            if (!readyLatch.await(3, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Timed out waiting for order Kafka producer");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for order Kafka producer", e);
        }
    }
}
