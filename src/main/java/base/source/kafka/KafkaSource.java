package base.source.kafka;

import base.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class KafkaSource<T> implements Source<T> {
    private final FlinkKafkaConsumer<T> kafkaConsumer;

    public KafkaSource(FlinkKafkaConsumer<T> consumer) {
        this.kafkaConsumer = consumer;
    }

    @Override
    public DataStream<T> getSource(StreamExecutionEnvironment env) {
        return env.addSource(kafkaConsumer);
    }
}
