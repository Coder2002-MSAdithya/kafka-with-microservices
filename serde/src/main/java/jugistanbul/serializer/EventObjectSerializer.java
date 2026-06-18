package jugistanbul.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import jugistanbul.entity.EventObject;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class EventObjectSerializer implements Serializer<EventObject> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @Override
    public byte[] serialize(final String topic, final EventObject data) {
        if (data == null) {
            return null;
        }
        try {
            return mapper.writeValueAsBytes(data);
        } catch (final Exception ex) {
            throw new IllegalStateException("Failed to serialize EventObject for topic " + topic, ex);
        }
    }

    @Override
    public void close() {
    }
}
