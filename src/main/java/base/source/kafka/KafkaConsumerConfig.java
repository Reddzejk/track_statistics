package base.source.kafka;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.util.List;
import java.util.Properties;

public class KafkaConsumerConfig<T> {
    private final List<String> topics;
    private final KeyedDeserializationSchema<T> deserializationSchema;
    private final Properties properties;

    public KafkaConsumerConfig(List<String> topics, KeyedDeserializationSchema<T> deserializationSchema, Properties properties) {
        this.topics = topics;
        this.deserializationSchema = deserializationSchema;
        this.properties = properties;
    }

    public FlinkKafkaConsumer<T> createConsumer() {
        return new FlinkKafkaConsumer<>(topics, deserializationSchema, properties);
    }
}
