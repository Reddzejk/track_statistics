package base;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public interface Source<T> {
    DataStream<T> getSource(StreamExecutionEnvironment env);
}
