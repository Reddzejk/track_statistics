package statistics.ranking;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

public class RankingPerDay<T> implements Serializable {
    private final LocalDate date;
    private final List<Tuple2<T, Long>> occurrences;


    public RankingPerDay(LocalDate date, List<Tuple2<T, Long>> occurrences) {
        this.date = date;
        this.occurrences = occurrences;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankingPerDay<?> that = (RankingPerDay<?>) o;
        if (occurrences == null || that.occurrences == null) {
            return false;
        }

        return Objects.equals(date, that.date) &&
                Objects.equals(occurrences, that.occurrences);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, occurrences.size());
    }

    @Override
    public String toString() {
        return "RankingPerDay{" +
                "date=" + date +
                ", occurrences=" + occurrences.toString() +
                '}';
    }
}
