package statistics.parsing;

import converter.SongInformationTuple3;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.io.Serializable;

public class SongInformationTimestampExtractor extends AscendingTimestampExtractor<SongInformationTuple3> {
    @Override
    public long extractAscendingTimestamp(SongInformationTuple3 element) {
        return element.getTimestamp();
    }
}