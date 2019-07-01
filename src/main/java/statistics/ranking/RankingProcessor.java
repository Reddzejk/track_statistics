package statistics.ranking;

import base.Processor;
import converter.SongInformationTuple3;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import statistics.ranking.top.TopWindowProcessFunction;
import util.LocalDateConverter;
import util.RankingPerDay;

import java.time.LocalDate;

public abstract class RankingProcessor<T> implements Processor<SongInformationTuple3, RankingPerDay<T>> {
    private final TopWindowProcessFunction<T> processFunction;

    RankingProcessor(TopWindowProcessFunction<T> processFunction) {
        this.processFunction = processFunction;
    }

    @Override
    public DataStream<RankingPerDay<T>> processStream(DataStream<SongInformationTuple3> stream) {
        DataStream<Tuple3<LocalDate, T, Long>> countedOccurrences = countOccurrencePerDay(stream);
        return createRankingTop(countedOccurrences);
    }

    private DataStream<Tuple3<LocalDate, T, Long>> countOccurrencePerDay(DataStream<SongInformationTuple3> stream) {
        return stream.map(song -> new Tuple3<>(LocalDateConverter.toLocalDate(song.getTimestamp()), getKey(song), 1L))
                .returns(typeHintTuple())
                .keyBy(tuple -> tuple.f1)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .sum(2);
    }

    private DataStream<RankingPerDay<T>> createRankingTop(DataStream<Tuple3<LocalDate, T, Long>> stream) {
        return stream.keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(processFunction)
                .returns(typeHintRanking());
    }

    protected abstract T getKey(SongInformationTuple3 song);

    protected abstract TypeHint<RankingPerDay<T>> typeHintRanking();

    protected abstract TypeHint<Tuple3<LocalDate, T, Long>> typeHintTuple();
}
