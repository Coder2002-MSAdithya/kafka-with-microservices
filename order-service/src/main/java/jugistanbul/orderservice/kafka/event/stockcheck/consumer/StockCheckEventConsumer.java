package jugistanbul.orderservice.kafka.event.stockcheck.consumer;

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
public class StockCheckEventConsumer {

    private EventConsumer eventConsumer;

    @Inject
    private Logger logger;

    @Resource
    private ManagedExecutorService service;

    @Inject
    private Event<EventObject> events;

    @PostConstruct
    public void initConsumer() {
        logger.info("Initialize stock check event consumer...");
        eventConsumer = new EventConsumer(
                DifcConstants.PRINCIPAL_ORDER,
                DifcConstants.SERVICE_ORDER,
                "stockCheckEventConsumerGroup01",
                DifcConstants.TAG_STOCK,
                ev -> events.fire(ev),
                DifcConstants.TOPIC_STOCK_CHECK);
        service.execute(eventConsumer);
    }

    @PreDestroy
    public void shutdown() {
        if (eventConsumer != null) {
            eventConsumer.stop();
        }
    }
}
