package statistics.session;

import converter.SongInformationTuple3;
import io.flinkspector.core.collection.ExpectedRecords;
import io.flinkspector.core.collection.MatcherBuilder;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import statistics.parsing.SongInformationTimestampExtractor;
import statistics.testutils.DataStreamTestBaseJUnit5;
import statistics.testutils.TestLogParser;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

class StatisticsProcessorTest extends DataStreamTestBaseJUnit5 {
    private StatisticsProcessor statisticsProcessor;

    @BeforeEach
    void setUp() {
        statisticsProcessor = new StatisticsProcessor();
    }


    @Test
    void shouldReturn2RecordsWithCountedStats() {
        List<String> logs = Arrays.asList(
                "userId=John, songId=5, startTime=\"2016-10-12T10:23:15Z\"",
                "userId=John, songId=8, startTime=\"2016-10-12T10:25:15Z\"",
                "userId=John, songId=5, startTime=\"2016-10-12T23:59:15Z\"",
                "userId=John, songId=13, startTime=\"2016-10-13T00:01:15Z\"",
                "userId=Eva, songId=13, startTime=\"2016-10-13T10:25:15Z\"",
                "userId=Eva, songId=7, startTime=\"2016-10-13T10:29:15Z\""
        );
        List<SongInformationTuple3> parsedLogs = TestLogParser.parse(logs);

        DataStream<SongInformationTuple3> source = testEnv.fromCollection(parsedLogs)
                .assignTimestampsAndWatermarks(new SongInformationTimestampExtractor());

        DataStream<Tuple5<LocalDate, Long, Long, Long, Long>> processedStream = statisticsProcessor.processStream(source);
        MatcherBuilder<Tuple5<LocalDate, Long, Long, Long, Long>> expectedRecords =
                new ExpectedRecords<Tuple5<LocalDate, Long, Long, Long, Long>>()
                        .expect(new Tuple5<>(LocalDate.parse("2016-10-12"), 120L, 120L, 120L, 120L))
                        .expect(new Tuple5<>(LocalDate.parse("2016-10-13"), 120L, 240L, 180L, 180L))
                        .refine()
                        .only();

        assertStream(processedStream, expectedRecords);
    }
    @Test
    void shouldDontReturnAnyStats() {
        List<String> logs = Arrays.asList(
                "userId=John, songId=5, startTime=\"2016-10-12T10:23:15Z\"",
                "userId=Eva, songId=7, startTime=\"2016-10-13T10:29:15Z\""
        );
        List<SongInformationTuple3> parsedLogs = TestLogParser.parse(logs);

        DataStream<SongInformationTuple3> source = testEnv.fromCollection(parsedLogs)
                .assignTimestampsAndWatermarks(new SongInformationTimestampExtractor());

        DataStream<Tuple5<LocalDate, Long, Long, Long, Long>> processedStream = statisticsProcessor.processStream(source);

        MatcherBuilder<Tuple5<LocalDate, Long, Long, Long, Long>> expectedEmptyStream =
                new ExpectedRecords<Tuple5<LocalDate, Long, Long, Long, Long>>()
                        .refine()
                        .only();

        assertStream(processedStream, expectedEmptyStream);
    }
}