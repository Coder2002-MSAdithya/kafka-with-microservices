package jugistanbul.difc;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.message.PollPrivsReqResponseData;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Background polling of the tag-owner {@code POLL_PRIVS_REQ} queue via a DIFC {@link KafkaProducer}.
 */
public final class DifcPrivilegePoller implements AutoCloseable {

    private static final long POLL_INTERVAL_MS = 500L;

    private final Logger log;
    private final String serviceName;
    private final KafkaProducer<?, ?> producer;
    private final GrantPrivilegeHandler handler;
    private final ScheduledExecutorService executor;

    public DifcPrivilegePoller(
            final Logger log,
            final String serviceName,
            final KafkaProducer<?, ?> producer,
            final GrantPrivilegeHandler handler) {
        this.log = Objects.requireNonNull(log, "log");
        this.serviceName = Objects.requireNonNull(serviceName, "serviceName");
        this.producer = Objects.requireNonNull(producer, "producer");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r, "difc-privilege-poller-" + serviceName);
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        executor.scheduleAtFixedRate(this::pollOnce, 0L, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void pollOnce() {
        try {
            final PollPrivsReqResponseData pending = producer.pollPrivsReq();
            if (pending == null) {
                return;
            }
            if (pending.capability() >= 0
                    && pending.tagName() != null
                    && !pending.tagName().isEmpty()) {
                log.info(
                        "[DIFC] pollPrivsReq pending service={} tag={} capability={} requester={}",
                        serviceName,
                        pending.tagName(),
                        pending.capability(),
                        pending.requesterClientId());
            }
            handler.onPrivilegeRequest(pending);
        } catch (final Exception e) {
            log.warn("[DIFC] pollPrivsReq failed service={}", serviceName, e);
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
