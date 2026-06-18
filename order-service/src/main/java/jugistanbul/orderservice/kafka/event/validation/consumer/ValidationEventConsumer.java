package jugistanbul.orderservice.kafka.event.validation.consumer;

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
public class ValidationEventConsumer {

    private EventConsumer eventConsumer;

    @Inject
    private Logger logger;

    @Resource
    private ManagedExecutorService service;

    @Inject
    private Event<EventObject> events;

    @PostConstruct
    public void initConsumer() {
        logger.info("Initialize validation event consumer...");
        eventConsumer = new EventConsumer(
                DifcConstants.PRINCIPAL_ORDER,
                DifcConstants.SERVICE_ORDER,
                "validationEventConsumerGroup01",
                DifcConstants.TAG_VALIDATION,
                ev -> events.fire(ev),
                DifcConstants.TOPIC_VALIDATION);
        service.execute(eventConsumer);
    }

    @PreDestroy
    public void shutdown() {
        if (eventConsumer != null) {
            eventConsumer.stop();
        }
    }
}
