package base;

import org.apache.flink.streaming.api.datastream.DataStream;

public class SubFlux<I, O> {
    private final DataStream<I> sourceStream;
    private final Processor<I, O> processor;
    private final Sink<O> sink;

    public SubFlux(DataStream<I> sourceStream, Processor<I, O> processor, Sink<O> streamSink) {
        this.sourceStream = sourceStream;
        this.processor = processor;
        this.sink = streamSink;
    }

    public void buildSubFlux() {
        DataStream<O> processStream = processor.processStream(sourceStream);
        sink.applySink(processStream);
    }

}
