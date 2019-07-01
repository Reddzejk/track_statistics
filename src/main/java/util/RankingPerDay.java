package util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.time.LocalDate;
import java.util.List;

public class RankingPerDay<T> {
    private final LocalDate date;
    private final List<Tuple2<T, Long>> occurrences;


    public RankingPerDay(LocalDate date, List<Tuple2<T, Long>> occurrences) {
        this.date = date;
        this.occurrences = occurrences;
    }

    public LocalDate getDate() {
        return date;
    }

    public List<Tuple2<T, Long>> getOccurrences() {
        return occurrences;
    }

    @Override
    public String toString() {
        return "RankingPerDay{" +
                "date=" + date +
                ", occurrences=" + occurrences.toString() +
                '}';
    }
}
