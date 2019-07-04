package base.sink;

import base.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

public class SongCassandraSink<T> implements Sink<T> {

    //TODO: Finish Config Cassandra
    @Override
    public void applySink(DataStream<T> toSinkStream) throws Exception {
        CassandraSink.addSink(toSinkStream).build();
    }
}
