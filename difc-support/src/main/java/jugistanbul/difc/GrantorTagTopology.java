package jugistanbul.difc;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/** Maps grantor-owned tags to publish topics and sensitive fields. */
public final class GrantorTagTopology {

    private GrantorTagTopology() {
    }

    public static List<String> outputTopicsForTag(final String tagName) {
        if (tagName == null || tagName.isEmpty()) {
            return Collections.emptyList();
        }
        return switch (tagName) {
            case DifcConstants.TAG_ORDER, DifcConstants.TAG_CARD -> List.of(DifcConstants.TOPIC_ORDER);
            case DifcConstants.TAG_STOCK -> List.of(DifcConstants.TOPIC_STOCK_CHECK);
            case DifcConstants.TAG_VALIDATION -> List.of(DifcConstants.TOPIC_VALIDATION);
            case DifcConstants.TAG_BILLING -> List.of(DifcConstants.TOPIC_BILLING);
            default -> Collections.emptyList();
        };
    }

    /**
     * Topics where this grantor's producer attaches the tag on write (strict publish set).
     */
    public static List<String> grantorPublishTopicsForTag(
            final String grantorPrincipal,
            final String tagName) {
        if (!ownsTag(grantorPrincipal, tagName)) {
            return Collections.emptyList();
        }
        return outputTopicsForTag(tagName);
    }

    /**
     * Pipeline topics on which consumed records may still carry {@code tagName} (including after
     * upstream republication). Used for {@code CAN_REMOVE} verification — not the same as
     * {@link #grantorPublishTopicsForTag}: e.g. {@code card} set by {@code order-svc} on
     * {@code ORDER_EVENT_TOPIC} can still be present on {@code STOCK_CHECK_EVENT_TOPIC} after
     * {@code stock-svc} republishes without stripping {@code card}.
     */
    public static List<String> tagCarrierTopics(final String tagName) {
        if (tagName == null || tagName.isEmpty()) {
            return Collections.emptyList();
        }
        return switch (tagName) {
            case DifcConstants.TAG_ORDER -> List.of(DifcConstants.TOPIC_ORDER);
            case DifcConstants.TAG_CARD ->
                    List.of(DifcConstants.TOPIC_ORDER, DifcConstants.TOPIC_STOCK_CHECK);
            case DifcConstants.TAG_STOCK -> List.of(DifcConstants.TOPIC_STOCK_CHECK);
            case DifcConstants.TAG_VALIDATION -> List.of(DifcConstants.TOPIC_VALIDATION);
            case DifcConstants.TAG_BILLING -> List.of(DifcConstants.TOPIC_BILLING);
            default -> Collections.emptyList();
        };
    }

    public static boolean ownsTag(final String grantorPrincipal, final String tagName) {
        if (grantorPrincipal == null || tagName == null) {
            return false;
        }
        return switch (tagName) {
            case DifcConstants.TAG_ORDER, DifcConstants.TAG_CARD ->
                    DifcConstants.PRINCIPAL_ORDER.equals(grantorPrincipal);
            case DifcConstants.TAG_STOCK -> DifcConstants.PRINCIPAL_STOCK.equals(grantorPrincipal);
            case DifcConstants.TAG_VALIDATION ->
                    DifcConstants.PRINCIPAL_VALIDATION.equals(grantorPrincipal);
            case DifcConstants.TAG_BILLING -> DifcConstants.PRINCIPAL_PAYMENT.equals(grantorPrincipal);
            default -> false;
        };
    }

    public static Set<String> sensitiveFieldsForTaggedInput(final String tagName, final String inputTopic) {
        if (tagName == null || inputTopic == null || inputTopic.isEmpty()) {
            return Set.of();
        }
        if (!tagCarrierTopics(tagName).contains(inputTopic)
                && !inputTopic.equals(topicForUpstreamTag(tagName))) {
            return Set.of();
        }
        if (DifcConstants.TAG_ORDER.equals(tagName)) {
            return TopicSchemaCatalog.sensitiveOrderFields();
        }
        if (DifcConstants.TAG_CARD.equals(tagName)) {
            return TopicSchemaCatalog.sensitiveCardFields();
        }
        return TopicSchemaCatalog.valueFieldsForTopic(inputTopic);
    }

    public static String principalForServiceName(final String serviceName) {
        return switch (serviceName) {
            case DifcConstants.SERVICE_ORDER -> DifcConstants.PRINCIPAL_ORDER;
            case DifcConstants.SERVICE_STOCK -> DifcConstants.PRINCIPAL_STOCK;
            case DifcConstants.SERVICE_VALIDATION -> DifcConstants.PRINCIPAL_VALIDATION;
            case DifcConstants.SERVICE_PAYMENT -> DifcConstants.PRINCIPAL_PAYMENT;
            default -> serviceName;
        };
    }

    private static String topicForUpstreamTag(final String tagName) {
        return switch (tagName) {
            case DifcConstants.TAG_STOCK -> DifcConstants.TOPIC_STOCK_CHECK;
            case DifcConstants.TAG_VALIDATION -> DifcConstants.TOPIC_VALIDATION;
            case DifcConstants.TAG_BILLING -> DifcConstants.TOPIC_BILLING;
            default -> "";
        };
    }
}
