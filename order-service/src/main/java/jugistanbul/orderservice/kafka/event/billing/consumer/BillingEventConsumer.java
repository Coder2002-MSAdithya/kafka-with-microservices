package jugistanbul.orderservice.kafka.event.billing.consumer;

import jugistanbul.difc.DifcConstants;
import jugistanbul.entity.EventObject;
import jugistanbul.orderservice.kafka.consumer.EventConsumer;
import org.slf4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.enterprise.event.Event;
import javax.inject.Inject;

@Startup
@Singleton
public class BillingEventConsumer {

    private EventConsumer eventConsumer;

    @Inject
    private Logger logger;

    @Resource
    private ManagedExecutorService service;

    @Inject
    private Event<EventObject> events;

    @PostConstruct
    public void initConsumer() {
        logger.info("Initialize billing event consumer...");
        eventConsumer = new EventConsumer(
                DifcConstants.PRINCIPAL_ORDER,
                DifcConstants.SERVICE_ORDER,
                "billingEventConsumerGroup01",
                DifcConstants.TAG_BILLING,
                ev -> events.fire(ev),
                DifcConstants.TOPIC_BILLING);
        service.execute(eventConsumer);
    }

    @PreDestroy
    public void shutdown() {
        if (eventConsumer != null) {
            eventConsumer.stop();
        }
    }
}
