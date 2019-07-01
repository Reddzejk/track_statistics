package base;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;

public interface Processor<I, O> extends Serializable {
    DataStream<O> processStream(DataStream<I> stream);
}
