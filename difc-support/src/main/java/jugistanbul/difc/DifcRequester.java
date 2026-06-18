package jugistanbul.difc;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.message.AddTagResponseData;
import org.apache.kafka.common.message.CreateTagResponseData;
import org.apache.kafka.common.message.DestroyTagResponseData;
import org.apache.kafka.common.message.GrantCapResponseData;
import org.apache.kafka.common.message.RegisterClientResponseData;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Requester-side DIFC registration, tag creation, and {@code GRANT_CAP} helpers. */
public final class DifcRequester {

    private DifcRequester() {
    }

    public static void registerClient(final Logger log, final String serviceName, final KafkaProducer<?, ?> producer) {
        final RegisterClientResponseData response = producer.registerClient();
        logRegisterClient(log, serviceName, response);
        if (response.errorCode() == 133) {
            return;
        }
        DifcOps.requireOk(response.errorCode(), "registerClient(" + serviceName + ")", response.errorMessage());
    }

    public static void registerClient(final Logger log, final String serviceName, final KafkaConsumer<?, ?> consumer) {
        final RegisterClientResponseData response = consumer.registerClient();
        logRegisterClient(log, serviceName, response);
        if (response.errorCode() == 133) {
            return;
        }
        DifcOps.requireOk(response.errorCode(), "registerClient(" + serviceName + ")", response.errorMessage());
    }

    public static void createTag(
            final Logger log,
            final String serviceName,
            final KafkaProducer<?, ?> producer,
            final String tagName) {
        CreateTagResponseData response = producer.createTag(tagName);
        logCreateTag(log, serviceName, tagName, response);
        if (response.errorCode() == 131) {
            final DestroyTagResponseData destroyed = producer.destroyTag(tagName);
            logDestroyTag(log, serviceName, tagName, destroyed);
            if (destroyed.errorCode() == 0) {
                response = producer.createTag(tagName);
                logCreateTag(log, serviceName, tagName, response);
            }
        }
        if (response.errorCode() == 131) {
            return;
        }
        DifcOps.requireOk(
                response.errorCode(),
                "createTag(" + serviceName + ", " + tagName + ")",
                response.errorMessage());
    }

    public static void requestGrantCap(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String tagName,
            final Capability capability) {
        final int maxAttempts = 90;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            final GrantCapResponseData response = capability == Capability.CAN_ADD
                    ? consumer.requestAddCapabilityForTag(tagName)
                    : consumer.requestRemoveCapabilityForTag(tagName);
            logRequestGrantCap(log, serviceName, tagName, capability, response);
            if (response.errorCode() == 0) {
                return;
            }
            if (response.errorCode() == 132 && attempt < maxAttempts) {
                log.warn(
                        "[DIFC] requestGrantCap retry service={} tag={} capability={} attempt={}/{} reason=tag not found",
                        serviceName,
                        tagName,
                        capability,
                        attempt,
                        maxAttempts);
                sleepQuietly(2000L);
                continue;
            }
            DifcOps.requireOk(
                    response.errorCode(),
                    "requestGrantCap(" + serviceName + ", " + tagName + ", " + capability + ")",
                    response.errorMessage());
        }
    }

    public static void requestGrantCapForConsume(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String tagName) {
        requestGrantCap(log, serviceName, consumer, tagName, Capability.CAN_ADD);
        waitForLabelTag(log, serviceName, consumer, tagName);
    }

    public static void requestGrantCapAddAndRemove(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String tagName) {
        requestGrantCap(log, serviceName, consumer, tagName, Capability.CAN_ADD);
        completeCanRemoveGrant(log, serviceName, consumer, tagName);
    }

    public static void completeCanRemoveGrant(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String tagName) {
        waitForAttestedProcessingPolicy(log, serviceName);
        try {
            DifcPolicyPublisher.publishAttestedPolicyForGrant();
        } catch (final IOException e) {
            throw new IllegalStateException(
                    "Failed to publish attested processing policy before CAN_REMOVE on tag " + tagName, e);
        }
        requestGrantCap(log, serviceName, consumer, tagName, Capability.CAN_REMOVE);
    }

    public static void requestGrantCapAdd(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String tagName) {
        requestGrantCap(log, serviceName, consumer, tagName, Capability.CAN_ADD);
        waitForLabelTag(log, serviceName, consumer, tagName);
    }

    public static void waitForLabelTag(
            final Logger log,
            final String serviceName,
            final KafkaConsumer<?, ?> consumer,
            final String tagName) {
        final int maxAttempts = 90;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            final AddTagResponseData label = consumer.addTag(tagName);
            if (label.errorCode() == 0) {
                logAddTag(log, serviceName, tagName, label);
                return;
            }
            log.warn(
                    "[DIFC] addTag label service={} tag={} errorCode={} message={} attempt={}/{}",
                    serviceName,
                    tagName,
                    label.errorCode(),
                    label.errorMessage(),
                    attempt,
                    maxAttempts);
            if (attempt == maxAttempts) {
                DifcOps.requireOk(label.errorCode(), "addTag(" + serviceName + ", " + tagName + ")", label.errorMessage());
            }
            sleepQuietly(2000L);
        }
    }

    public static void waitForAttestedProcessingPolicy(final Logger log, final String serviceName) {
        final String principal = System.getProperty("policy.app.principal");
        if (principal == null || principal.isEmpty()) {
            log.info("[DIFC] policyWaitSkipped service={} reason=no policy.app.principal", serviceName);
            return;
        }
        final Path policyPath = DifcTagPolicyVerifier.policyPathForRequester(principal);
        final int maxAttempts = 60;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (Files.isRegularFile(policyPath)) {
                log.info(
                        "[DIFC] attestedPolicyReady service={} principal={} path={}",
                        serviceName,
                        principal,
                        policyPath);
                return;
            }
            sleepQuietly(1000L);
        }
        throw new IllegalStateException(
                "Attested processing policy not found at " + policyPath + " for service " + serviceName);
    }

    private static void logRegisterClient(
            final Logger log,
            final String serviceName,
            final RegisterClientResponseData response) {
        final String line = String.format(
                "[DIFC] registerClient service=%s errorCode=%d message=%s",
                serviceName,
                response.errorCode(),
                response.errorMessage());
        log.info(line);
        System.out.println(line);
    }

    private static void logCreateTag(
            final Logger log,
            final String serviceName,
            final String tagName,
            final CreateTagResponseData response) {
        final String line = String.format(
                "[DIFC] createTag service=%s tag=%s errorCode=%d message=%s",
                serviceName,
                tagName,
                response.errorCode(),
                response.errorMessage());
        log.info(line);
        System.out.println(line);
    }

    private static void logDestroyTag(
            final Logger log,
            final String serviceName,
            final String tagName,
            final DestroyTagResponseData response) {
        final String line = String.format(
                "[DIFC] destroyTag service=%s tag=%s errorCode=%d message=%s",
                serviceName,
                tagName,
                response.errorCode(),
                response.errorMessage());
        log.info(line);
        System.out.println(line);
    }

    private static void logAddTag(
            final Logger log,
            final String serviceName,
            final String tagName,
            final AddTagResponseData response) {
        final String line = String.format(
                "[DIFC] addTag service=%s tag=%s errorCode=%d message=%s",
                serviceName,
                tagName,
                response.errorCode(),
                response.errorMessage());
        log.info(line);
        System.out.println(line);
    }

    private static void logRequestGrantCap(
            final Logger log,
            final String serviceName,
            final String tagName,
            final Capability capability,
            final GrantCapResponseData response) {
        final String line = String.format(
                "[DIFC] requestGrantCap service=%s tag=%s capability=%s errorCode=%d message=%s",
                serviceName,
                tagName,
                capability,
                response.errorCode(),
                response.errorMessage());
        log.info(line);
        System.out.println(line);
    }

    private static void sleepQuietly(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for DIFC grant", e);
        }
    }

    // Kept for compatibility if Streams is used elsewhere.
    public static void registerClient(final Logger log, final String serviceName, final KafkaStreams streams) {
        final RegisterClientResponseData response = streams.registerClient();
        logRegisterClient(log, serviceName, response);
        if (response.errorCode() == 133) {
            return;
        }
        DifcOps.requireOk(response.errorCode(), "registerClient(" + serviceName + ")", response.errorMessage());
    }
}
