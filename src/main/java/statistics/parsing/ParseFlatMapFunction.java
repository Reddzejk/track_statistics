package statistics.parsing;

import converter.SongInformationTuple3;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseFlatMapFunction implements FlatMapFunction<String, SongInformationTuple3> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseFlatMapFunction.class);

    @Override
    public void flatMap(String value, Collector<SongInformationTuple3> out) throws Exception {
        try {
            SongInformationTuple3 parsed = SongInformationTuple3.parse(value);
            out.collect(parsed);
        } catch (Exception e) {
            LOG.error("Parsed failure {}. Error: {}", value, e.getMessage());
        }
    }
}
