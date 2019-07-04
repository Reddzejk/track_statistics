package statistics.session.global;

import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDate;
import java.util.List;

public class DayStatisticsWindowProcessFunction extends ProcessWindowFunction<Tuple2<LocalDate, Long>, Tuple5<LocalDate, Long, Long, Long, Long>, LocalDate, TimeWindow> {
    @Override
    public void process(LocalDate key, Context context, Iterable<Tuple2<LocalDate, Long>> elements, Collector<Tuple5<LocalDate, Long, Long, Long, Long>> out) {
        Ordering<Tuple2<LocalDate, Long>> ordering = new SessionOrdering<>();
        List<Tuple2<LocalDate, Long>> sorted = ordering.sortedCopy(elements);

        long min = toSeconds(sorted.get(0).f1);
        long max = toSeconds(sorted.get(sorted.size() - 1).f1);
        long avg = countAvg(sorted);
        long median = countMedian(sorted);

        out.collect(new Tuple5<>(key, min, max, avg, median));
    }

    private long countAvg(List<Tuple2<LocalDate, Long>> list) {
        long avg = list.stream().mapToLong(tuple -> tuple.f1).sum() / list.size();
        return toSeconds(avg);
    }

    private long countMedian(List<Tuple2<LocalDate, Long>> list) {
        long result;
        if (list.size() % 2 == 0) {
            result = (list.get(list.size() / 2).f1 + list.get(list.size() / 2 - 1).f1) / 2;
        } else {
            result = list.get(list.size() / 2).f1;
        }
        return toSeconds(result);
    }

    private long toSeconds(long l) {
        return Duration.ofMillis(l).getSeconds();
    }
}

