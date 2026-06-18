package jugistanbul.orderservice.kafka.publish;

import jugistanbul.difc.DifcConstants;
import jugistanbul.difc.KafkaClientConfig;
import jugistanbul.entity.EventObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@ApplicationScoped
public class KafkaPublisher {

    @Inject
    private Logger logger;

    @Inject
    private KafkaProducer<Integer, EventObject> producer;

    public KafkaPublisher() {
    }

    public void publishRecordToKafka(final String topic, final EventObject orderEvent) {
        final ProducerRecord<Integer, EventObject> record =
                new ProducerRecord<>(topic, orderEvent.getCustomerId(), orderEvent);
        try {
            producer.beginTransaction();
            final RecordMetadata metadata;
            if (KafkaClientConfig.difcEnabled()) {
                metadata = producer.sendWithTags(
                        record,
                        Set.of(DifcConstants.TAG_ORDER, DifcConstants.TAG_CARD),
                        null).get();
            } else {
                metadata = producer.send(record).get();
            }
            producer.commitTransaction();
            logger.info(
                    "Order event published: topic={} partition={} offset={}",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted publishing order event", e);
        } catch (ExecutionException e) {
            logger.error("Failed publishing order event", e.getCause());
        } catch (ProducerFencedException e) {
            logger.error("Producer fenced", e);
            producer.close();
        } catch (KafkaException e) {
            logger.error("Kafka error publishing order event", e);
            producer.abortTransaction();
        }
    }
}
