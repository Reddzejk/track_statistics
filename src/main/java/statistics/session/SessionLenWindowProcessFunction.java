package statistics.session;

import com.google.common.collect.Ordering;
import converter.LocalDateConverter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import statistics.session.global.SessionOrdering;

import java.time.LocalDate;

public class SessionLenWindowProcessFunction extends ProcessWindowFunction<Tuple2<String, Long>, Tuple2<LocalDate, Long>, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<LocalDate, Long>> out) throws Exception {
        Ordering<Tuple2<String, Long>> ordering = new SessionOrdering<>();
        Long minLength = ordering.min(elements).f1;
        Long maxLength = ordering.max(elements).f1;
        long sessionLength = maxLength - minLength;

        if (sessionLength != 0) {
            out.collect(new Tuple2<>(LocalDateConverter.toLocalDate(maxLength), sessionLength));
        }
    }
}
