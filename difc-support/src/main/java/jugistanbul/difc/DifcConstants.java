package jugistanbul.difc;

/** DIFC principals, service names, tags, and topics for the JUG Istanbul pipeline demo. */
public final class DifcConstants {

    public static final String TAG_ORDER = "order";
    public static final String TAG_CARD = "card";
    public static final String TAG_STOCK = "stock";
    public static final String TAG_VALIDATION = "validation";
    public static final String TAG_BILLING = "billing";

    public static final String PRINCIPAL_ORDER = "order-svc";
    public static final String PRINCIPAL_STOCK = "stock-svc";
    public static final String PRINCIPAL_VALIDATION = "validation-svc";
    public static final String PRINCIPAL_PAYMENT = "payment-svc";

    public static final String SERVICE_ORDER = "OrderService";
    public static final String SERVICE_STOCK = "StockService";
    public static final String SERVICE_VALIDATION = "ValidationService";
    public static final String SERVICE_PAYMENT = "PaymentService";

    public static final String TOPIC_ORDER = "ORDER_EVENT_TOPIC";
    public static final String TOPIC_STOCK_CHECK = "STOCK_CHECK_EVENT_TOPIC";
    public static final String TOPIC_VALIDATION = "VALIDATION_EVENT_TOPIC";
    public static final String TOPIC_BILLING = "BILLING_EVENT_TOPIC";

    public static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094");

    private DifcConstants() {
    }
}
