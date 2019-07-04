package statistics.testutils;

import converter.SongInformationTuple3;

import java.util.List;
import java.util.stream.Collectors;

public class TestLogParser {
    private TestLogParser() {
    }


    public static List<SongInformationTuple3> parse(List<String> logs) {
        return logs.stream()
                .map(SongInformationTuple3::parse)
                .collect(Collectors.toList());
    }

}
