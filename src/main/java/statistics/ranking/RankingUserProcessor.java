package statistics.ranking;

import converter.SongInformationTuple3;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import statistics.ranking.top.TopWindowProcessFunction;

import java.time.LocalDate;

public class RankingUserProcessor extends RankingProcessor<String> {

    public RankingUserProcessor(TopWindowProcessFunction<String> processFunction) {
        super(processFunction);
    }

    @Override
    protected final String getKey(SongInformationTuple3 song) {
        return song.getUserId();
    }

    @Override
    protected final TypeHint<RankingPerDay<String>> typeHintRanking() {
        return new TypeHint<RankingPerDay<String>>() {
        };
    }

    @Override
    protected final TypeHint<Tuple3<LocalDate, String, Long>> typeHintTuple() {
        return new TypeHint<Tuple3<LocalDate, String, Long>>() {
        };
    }
}
