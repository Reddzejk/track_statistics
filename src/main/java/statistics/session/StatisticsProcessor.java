package statistics.session;

import base.Processor;
import converter.SongInformationTuple3;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import statistics.session.global.DayStatisticsWindowProcessFunction;

import java.time.LocalDate;

/**
 * Processor count Min, Max, Avg and Median from 15-minutes Session
 */
public class StatisticsProcessor implements Processor<SongInformationTuple3, Tuple5<LocalDate, Long, Long, Long, Long>> {
    @Override
    public DataStream<Tuple5<LocalDate, Long, Long, Long, Long>> processStream(DataStream<SongInformationTuple3> stream) {
        DataStream<Tuple2<LocalDate, Long>> countedSessionLength = countSessionsLength(stream);
        return countDayStatistics(countedSessionLength);
    }

    private DataStream<Tuple2<LocalDate, Long>> countSessionsLength(DataStream<SongInformationTuple3> stream) {
        return stream.map(songInfo -> new Tuple2<>(songInfo.getUserId(), songInfo.getTimestamp())).
                returns(new TypeHint<Tuple2<String, Long>>() {
                }).keyBy(tuple -> tuple.f0)
                .window(EventTimeSessionWindows.withGap(Time.minutes(15)))
                .process(new SessionLenWindowProcessFunction());
    }

    private DataStream<Tuple5<LocalDate, Long, Long, Long, Long>> countDayStatistics(DataStream<Tuple2<LocalDate, Long>> stream) {
        return stream.keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new DayStatisticsWindowProcessFunction());
    }
}
