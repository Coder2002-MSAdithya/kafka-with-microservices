package jugistanbul.entity;

/**
 * Field projections between pipeline stages so each egress topic carries only the
 * columns attested in the processing policy.
 */
public final class EventProjections {

    private EventProjections() {
    }

    /** Stock stage: keep inventory context + card for validation; drop order price. */
    public static EventObject forStockCheck(final EventObject in, final boolean inStock) {
        final EventObject out = new EventObject();
        out.setCustomerId(in.getCustomerId());
        out.setProductId(in.getProductId());
        out.setAmount(in.getAmount());
        out.setCardNumber(in.getCardNumber());
        out.setInStock(inStock);
        out.setEvent("stock-check");
        return out;
    }

    /** Validation stage: decision only; card stripped. */
    public static EventObject forValidation(final EventObject in, final boolean numberValid) {
        final EventObject out = new EventObject();
        out.setCustomerId(in.getCustomerId());
        out.setNumberValid(numberValid);
        out.setEvent("validation");
        return out;
    }

    /** Billing stage: billed amount only. */
    public static EventObject forBilling(final EventObject in, final int price) {
        final EventObject out = new EventObject();
        out.setCustomerId(in.getCustomerId());
        out.setPrice(price);
        out.setEvent("billing");
        return out;
    }
}
