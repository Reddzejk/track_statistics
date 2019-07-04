package base;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface Flux<T> {
    DataStream<T> getSource(StreamExecutionEnvironment env);

    void buildFlux(StreamExecutionEnvironment env) throws Exception;
}
