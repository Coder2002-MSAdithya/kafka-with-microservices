package jugistanbul.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import jugistanbul.entity.EventObject;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class EventObjectDeserializer implements Deserializer<EventObject> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public EventObject deserialize(final String topic, final byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            return mapper.readValue(data, EventObject.class);
        } catch (final Exception ex) {
            return null;
        }
    }

    @Override
    public void close() {
    }
}
