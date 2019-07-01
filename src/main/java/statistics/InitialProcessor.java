package statistics;

import base.Processor;
import converter.SongInformationTuple3;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

//Todo: maybe we need other Assigner without anomalies logging
public class InitialProcessor implements Processor<String, SongInformationTuple3> {
    @Override
    public DataStream<SongInformationTuple3> processStream(DataStream<String> stream) {
        return stream.map(SongInformationTuple3::parse)
                .returns(new TypeHint<SongInformationTuple3>() {
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SongInformationTuple3>() {
                    @Override
                    public long extractAscendingTimestamp(SongInformationTuple3 element) {
                        return element.getTimestamp();
                    }
                });
    }
}
