package statistics.ranking.top;

import com.google.common.collect.Ordering;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.RankingPerDay;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class TopWindowProcessFunction<T> extends ProcessWindowFunction<Tuple3<LocalDate, T, Long>, RankingPerDay<T>, LocalDate, TimeWindow> {
    private final int topLength;

    public TopWindowProcessFunction(int topLength) {
        this.topLength = topLength;
    }

    @Override
    public void process(LocalDate key, Context context, Iterable<Tuple3<LocalDate, T, Long>> elements, Collector<RankingPerDay<T>> out) {
        out.collect(mapToRankingPerDay(key, elements));
    }

    private RankingPerDay<T> mapToRankingPerDay(LocalDate key, Iterable<Tuple3<LocalDate, T, Long>> elements) {
        List<Tuple2<T, Long>> top = new ArrayList<>();
        getNGreatest(elements).forEach(tuple3 -> top.add(new Tuple2<>(tuple3.f1, tuple3.f2)));
        return new RankingPerDay<>(key, top);
    }

    private List<Tuple3<LocalDate, T, Long>> getNGreatest(Iterable<Tuple3<LocalDate, T, Long>> elements) {
        Ordering<Tuple3<LocalDate, T, Long>> ordering = new TopOrdering<>();
        return ordering.greatestOf(elements, topLength);
    }


}
