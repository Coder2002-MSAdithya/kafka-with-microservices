package jugistanbul.difc;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Topic → record field catalog for grant-time field-sanitization analysis. */
public final class TopicSchemaCatalog {

    private static final Set<String> ORDER_SENSITIVE_FIELDS =
            Set.of("customerId", "productId", "amount", "price");

    private static final Set<String> CARD_SENSITIVE_FIELDS = Set.of("cardNumber");

    private static final Map<String, Set<String>> TOPIC_VALUE_FIELDS =
            Map.of(
                    DifcConstants.TOPIC_ORDER,
                    Set.of("customerId", "productId", "amount", "price", "cardNumber"),
                    DifcConstants.TOPIC_STOCK_CHECK,
                    Set.of("customerId", "productId", "amount", "inStock", "cardNumber"),
                    DifcConstants.TOPIC_VALIDATION,
                    Set.of("customerId", "isNumberValid"),
                    DifcConstants.TOPIC_BILLING,
                    Set.of("customerId", "price"));

    private TopicSchemaCatalog() {
    }

    public static Set<String> valueFieldsForTopic(final String topic) {
        if (topic == null) {
            return Set.of();
        }
        return TOPIC_VALUE_FIELDS.getOrDefault(topic, Collections.emptySet());
    }

  public static boolean isKnownTopic(final String topic) {
    return TOPIC_VALUE_FIELDS.containsKey(topic);
  }

    public static Set<String> sensitiveOrderFields() {
        return ORDER_SENSITIVE_FIELDS;
    }

    public static Set<String> sensitiveCardFields() {
        return CARD_SENSITIVE_FIELDS;
    }

    public static Set<String> copyFields(final Set<String> fields) {
        return new LinkedHashSet<>(fields);
    }
}
