package statistics.session.global;

import com.google.common.collect.Ordering;
import org.apache.flink.api.java.tuple.Tuple2;

public class SessionOrdering<T> extends Ordering<Tuple2<T, Long>> {
    @Override
    public int compare(Tuple2<T, Long> t1, Tuple2<T, Long> t2) {
        return Long.compare(t1.f1, t2.f1);
    }
}
