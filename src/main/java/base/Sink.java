package base;

import org.apache.flink.streaming.api.datastream.DataStream;

public interface Sink<O> {
    void applySink(DataStream<O> toSinkStream);
}
