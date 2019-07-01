package statistics.ranking;

import converter.SongInformationTuple3;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import statistics.ranking.top.TopWindowProcessFunction;
import util.RankingPerDay;

import java.time.LocalDate;

public class RankingSongProcessor extends RankingProcessor<Long> {

    public RankingSongProcessor(TopWindowProcessFunction<Long> processFunction) {
        super(processFunction);
    }

    @Override
    protected final Long getKey(SongInformationTuple3 song) {
        return song.getSongId();
    }

    @Override
    protected final TypeHint<RankingPerDay<Long>> typeHintRanking() {
        return new TypeHint<RankingPerDay<Long>>() {
        };
    }

    @Override
    protected final TypeHint<Tuple3<LocalDate, Long, Long>> typeHintTuple() {
        return new TypeHint<Tuple3<LocalDate, Long, Long>>() {
        };
    }
}
