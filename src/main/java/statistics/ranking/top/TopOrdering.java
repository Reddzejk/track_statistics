package statistics.ranking.top;

import com.google.common.collect.Ordering;
import org.apache.flink.api.java.tuple.Tuple3;

import java.time.LocalDate;

public class TopOrdering<T> extends Ordering<Tuple3<LocalDate, T, Long>> {
    public int compare(Tuple3<LocalDate, T, Long> t1, Tuple3<LocalDate, T, Long> t2) {
        return Long.compare(t1.f2, t2.f2);
    }
}
