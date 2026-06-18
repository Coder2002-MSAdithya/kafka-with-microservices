package jugistanbul.difc;

import org.apache.kafka.clients.Capability;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.message.AddClientPrivsResponseData;
import org.apache.kafka.common.message.CreateTagResponseData;
import org.apache.kafka.common.message.DestroyTagResponseData;
import org.apache.kafka.common.message.PollPrivsReqResponseData;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * Grantor-side approval of queued {@code GRANT_CAP} requests via {@code ADD_CLIENT_PRIVS}.
 */
public final class DifcGrantor {

    private DifcGrantor() {
    }

    public static Capability capabilityFromPollResponse(final byte capability) {
        return switch (capability) {
            case 0 -> Capability.CAN_ADD;
            case 1 -> Capability.CAN_REMOVE;
            default -> throw new IllegalArgumentException("Unknown DIFC capability code: " + capability);
        };
    }

    public static void grantPendingPrivilegeRequest(
            final Logger log,
            final String serviceName,
            final KafkaStreams streams,
            final PollPrivsReqResponseData pending) {
        if (pending == null || pending.capability() < 0) {
            return;
        }
        final String tagName = pending.tagName();
        final String requester = pending.requesterClientId();
        if (tagName == null || tagName.isEmpty() || requester == null || requester.isEmpty()) {
            return;
        }
        final Capability capability = capabilityFromPollResponse(pending.capability());
        if (!approveGrant(log, serviceName, requester, tagName, capability)) {
            return;
        }
        final AddClientPrivsResponseData response = streams.addClientPrivs(requester, tagName, capability);
        logGrantResult(log, serviceName, requester, tagName, capability, response);
    }

    public static void grantPendingPrivilegeRequest(
            final Logger log,
            final String serviceName,
            final KafkaProducer<?, ?> producer,
            final PollPrivsReqResponseData pending) {
        if (pending == null || pending.capability() < 0) {
            return;
        }
        final String tagName = pending.tagName();
        final String requester = pending.requesterClientId();
        if (tagName == null || tagName.isEmpty() || requester == null || requester.isEmpty()) {
            return;
        }
        final Capability capability = capabilityFromPollResponse(pending.capability());
        if (!approveGrant(log, serviceName, requester, tagName, capability)) {
            return;
        }
        final AddClientPrivsResponseData response = producer.addClientPrivs(requester, tagName, capability);
        logGrantResult(log, serviceName, requester, tagName, capability, response);
    }

    public static GrantPrivilegeHandler autoGrantForStreams(
            final Logger log,
            final String serviceName,
            final KafkaStreams streams) {
        return pending -> grantPendingPrivilegeRequest(log, serviceName, streams, pending);
    }

    public static GrantPrivilegeHandler autoGrantForProducer(
            final Logger log,
            final String serviceName,
            final KafkaProducer<?, ?> producer) {
        return pending -> grantPendingPrivilegeRequest(log, serviceName, producer, pending);
    }

    public static void preGrantAddConsumers(
            final Logger log,
            final String serviceName,
            final KafkaProducer<?, ?> producer,
            final String ownedTag) {
        for (final String requester : DifcGrantPolicy.allowedAddRequestersForTag(ownedTag)) {
            if (!DifcGrantPolicy.isPrincipalAllowedGrant(ownedTag, requester, Capability.CAN_ADD)) {
                continue;
            }
            final AddClientPrivsResponseData response =
                    producer.addClientPrivs(requester, ownedTag, Capability.CAN_ADD);
            final String line = String.format(
                    "[DIFC] preGrantAdd service=%s requester=%s tag=%s capability=%s errorCode=%d message=%s",
                    serviceName,
                    requester,
                    ownedTag,
                    Capability.CAN_ADD,
                    response.errorCode(),
                    response.errorMessage());
            if (response.errorCode() == 0) {
                log.info(line);
            } else {
                log.warn(line);
            }
            System.out.println(line);
        }
    }

    private static boolean approveGrant(
            final Logger log,
            final String serviceName,
            final String requester,
            final String tagName,
            final Capability capability) {
        if (!DifcGrantPolicy.isPrincipalAllowedGrant(tagName, requester, capability)) {
            log.warn(
                    "[DIFC] grantDenied service={} requester={} tag={} capability={} reason=principal not allowed",
                    serviceName,
                    requester,
                    tagName,
                    capability);
            return false;
        }
        if (capability == Capability.CAN_ADD) {
            try {
                final DifcTagPolicyVerifier.VerificationResult externalResult =
                        DifcExternalConnectionVerifier.verifyCanAdd(
                                DifcGrantPolicy.normalizePrincipal(requester));
                if (!externalResult.allowed()) {
                    log.warn(
                            "[DIFC] grantDeniedExternal service={} requester={} tag={} capability={} reason={}",
                            serviceName,
                            requester,
                            tagName,
                            capability,
                            externalResult.reason());
                    return false;
                }
                log.info(
                        "[DIFC] grantExternalVerified service={} requester={} tag={} capability={} reason={}",
                        serviceName,
                        requester,
                        tagName,
                        capability,
                        externalResult.reason());
            } catch (final IOException e) {
                log.info(
                        "[DIFC] grantExternalCheckSkipped service={} requester={} tag={} capability={} reason={}",
                        serviceName,
                        requester,
                        tagName,
                        capability,
                        e.getMessage());
            }
            return true;
        }
        if (capability != Capability.CAN_REMOVE) {
            return true;
        }
        try {
            final String grantorPrincipal = GrantorTagTopology.principalForServiceName(serviceName);
            final DifcTagPolicyVerifier.VerificationResult policyResult =
                    DifcTagPolicyVerifier.verifyAttestationAndCanRemoveOnTag(
                            requester, grantorPrincipal, tagName);
            if (!policyResult.allowed()) {
                log.warn(
                        "[DIFC] grantDeniedPolicy service={} requester={} tag={} capability={} reason={}",
                        serviceName,
                        requester,
                        tagName,
                        capability,
                        policyResult.reason());
                return false;
            }
            log.info(
                    "[DIFC] grantPolicyVerified service={} requester={} tag={} capability={} reason={}",
                    serviceName,
                    requester,
                    tagName,
                    capability,
                    policyResult.reason());
            return true;
        } catch (final IOException e) {
            log.warn(
                    "[DIFC] grantDeniedPolicy service={} requester={} tag={} capability={} reason={}",
                    serviceName,
                    requester,
                    tagName,
                    capability,
                    e.getMessage());
            return false;
        }
    }

    private static void logGrantResult(
            final Logger log,
            final String serviceName,
            final String requester,
            final String tagName,
            final Capability capability,
            final AddClientPrivsResponseData response) {
        final String line = String.format(
                "[DIFC] grantPrivilege service=%s requester=%s tag=%s capability=%s errorCode=%d message=%s",
                serviceName,
                requester,
                tagName,
                capability,
                response.errorCode(),
                response.errorMessage());
        if (response.errorCode() == 0) {
            log.info(line);
        } else {
            log.warn(line);
        }
        System.out.println(line);
    }
}
