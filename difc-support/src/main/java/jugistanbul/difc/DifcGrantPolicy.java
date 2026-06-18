package jugistanbul.difc;

import org.apache.kafka.clients.Capability;

/** Allow-list for tag owners approving {@code GRANT_CAP} requests. */
public final class DifcGrantPolicy {

    private DifcGrantPolicy() {
    }

    public static boolean isPrincipalAllowedGrant(
            final String tagName,
            final String requesterPrincipal,
            final Capability capability) {
        if (tagName == null || requesterPrincipal == null || capability == null) {
            return false;
        }
        return switch (capability) {
            case CAN_ADD -> isAllowedAddGrant(tagName, requesterPrincipal);
            case CAN_REMOVE -> isAllowedRemoveGrant(tagName, requesterPrincipal);
        };
    }

    public static java.util.List<String> allowedAddRequestersForTag(final String tagName) {
        return switch (tagName) {
            case DifcConstants.TAG_ORDER -> java.util.List.of(DifcConstants.PRINCIPAL_STOCK);
            case DifcConstants.TAG_CARD ->
                    java.util.List.of(DifcConstants.PRINCIPAL_STOCK, DifcConstants.PRINCIPAL_VALIDATION);
            case DifcConstants.TAG_STOCK ->
                    java.util.List.of(DifcConstants.PRINCIPAL_VALIDATION, DifcConstants.PRINCIPAL_ORDER);
            case DifcConstants.TAG_VALIDATION ->
                    java.util.List.of(DifcConstants.PRINCIPAL_PAYMENT, DifcConstants.PRINCIPAL_ORDER);
            case DifcConstants.TAG_BILLING -> java.util.List.of(DifcConstants.PRINCIPAL_ORDER);
            default -> java.util.List.of();
        };
    }

    private static boolean isAllowedAddGrant(final String tagName, final String requesterPrincipal) {
        final String principal = normalizePrincipal(requesterPrincipal);
        return switch (tagName) {
            case DifcConstants.TAG_ORDER -> DifcConstants.PRINCIPAL_STOCK.equals(principal);
            case DifcConstants.TAG_CARD ->
                    DifcConstants.PRINCIPAL_STOCK.equals(principal)
                            || DifcConstants.PRINCIPAL_VALIDATION.equals(principal);
            case DifcConstants.TAG_STOCK ->
                    DifcConstants.PRINCIPAL_VALIDATION.equals(principal)
                            || DifcConstants.PRINCIPAL_ORDER.equals(principal);
            case DifcConstants.TAG_VALIDATION ->
                    DifcConstants.PRINCIPAL_PAYMENT.equals(principal)
                            || DifcConstants.PRINCIPAL_ORDER.equals(principal);
            case DifcConstants.TAG_BILLING -> DifcConstants.PRINCIPAL_ORDER.equals(principal);
            default -> false;
        };
    }

    private static boolean isAllowedRemoveGrant(final String tagName, final String requesterPrincipal) {
        final String principal = normalizePrincipal(requesterPrincipal);
        return switch (tagName) {
            case DifcConstants.TAG_ORDER -> DifcConstants.PRINCIPAL_STOCK.equals(principal);
            case DifcConstants.TAG_CARD -> DifcConstants.PRINCIPAL_VALIDATION.equals(principal);
            case DifcConstants.TAG_STOCK -> DifcConstants.PRINCIPAL_VALIDATION.equals(principal);
            case DifcConstants.TAG_VALIDATION -> DifcConstants.PRINCIPAL_PAYMENT.equals(principal);
            default -> false;
        };
    }

    public static String normalizePrincipal(final String requesterClientId) {
        if (requesterClientId == null) {
            return "";
        }
        if (requesterClientId.endsWith("-consumer")) {
            return requesterClientId.substring(0, requesterClientId.length() - "-consumer".length());
        }
        if (requesterClientId.endsWith("-producer")) {
            return requesterClientId.substring(0, requesterClientId.length() - "-producer".length());
        }
        return requesterClientId;
    }
}
