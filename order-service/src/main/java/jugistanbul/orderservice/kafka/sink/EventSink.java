package jugistanbul.orderservice.kafka.sink;

import jugistanbul.entity.EventObject;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

@ApplicationScoped
public class EventSink {

    @Inject
    private Logger logger;

    public EventSink() {
    }

    public void onMessage(@Observes final EventObject event) {
        switch (event.getEvent()) {
            case "stock-check":
                if (!event.isInStock()) {
                    logger.info(
                            "Stock check: customer={} product={} is OUT of stock",
                            event.getCustomerId(),
                            event.getProductId());
                } else {
                    logger.info(
                            "Stock check: customer={} product={} is in stock",
                            event.getCustomerId(),
                            event.getProductId());
                }
                break;
            case "validation":
                logger.info(
                        "Validation: customer={} cardValid={}",
                        event.getCustomerId(),
                        event.isNumberValid());
                break;
            case "billing":
                logger.info("Billing: customer={} totalPrice={}", event.getCustomerId(), event.getPrice());
                break;
            default:
                break;
        }
    }
}
