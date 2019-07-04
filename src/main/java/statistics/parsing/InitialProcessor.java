package statistics.parsing;

import base.Processor;
import converter.SongInformationTuple3;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStream;

//Todo: maybe we need other Assigner without anomalies logging
public class InitialProcessor implements Processor<String, SongInformationTuple3> {

    @Override
    public DataStream<SongInformationTuple3> processStream(DataStream<String> stream) {
        return stream
                .flatMap(new ParseFlatMapFunction())
                .returns(new TypeHint<SongInformationTuple3>() {
                })
                .assignTimestampsAndWatermarks(new SongInformationTimestampExtractor());
    }

}
