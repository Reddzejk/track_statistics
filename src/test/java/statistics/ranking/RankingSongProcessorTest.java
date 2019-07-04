package statistics.ranking;

import converter.SongInformationTuple3;
import io.flinkspector.core.collection.ExpectedRecords;
import io.flinkspector.core.collection.MatcherBuilder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import statistics.parsing.SongInformationTimestampExtractor;
import statistics.ranking.top.TopWindowProcessFunction;
import statistics.testutils.DataStreamTestBaseJUnit5;
import statistics.testutils.TestLogParser;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

class RankingSongProcessorTest extends DataStreamTestBaseJUnit5 {
    private RankingSongProcessor rankingSongProcessor;
    private final int TOP_THREE = 3;

    @BeforeEach
    void setUp() {
        rankingSongProcessor = new RankingSongProcessor(new TopWindowProcessFunction<>(TOP_THREE));
    }


    @Test
    void shouldReturn2RecordsWithTopUsersPerDay() {
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


        RankingPerDay<Long> expected20161012 =
                new RankingPerDay<>(LocalDate.parse("2016-10-12"), Arrays.asList(Tuple2.of(5L, 2L), Tuple2.of(8L, 1L)));
        RankingPerDay<Long> expected20161013 =
                new RankingPerDay<>(LocalDate.parse("2016-10-13"), Arrays.asList(Tuple2.of(13L, 2L), Tuple2.of(7L, 1L)));

        DataStream<RankingPerDay<Long>> processedStream = rankingSongProcessor.processStream(source);

        MatcherBuilder<RankingPerDay<Long>> expectedRecords =
                new ExpectedRecords<RankingPerDay<Long>>()
                        .expect(expected20161012)
                        .expect(expected20161013)
                        .refine()
                        .only();

        assertStream(processedStream, expectedRecords);
    }

    @Test
    void shouldReturnTop3RecordsWithSongsPerDayAndIgnoreOtherSongs() {
        List<String> logs = Arrays.asList(
                "userId=John, songId=1, startTime=\"2016-10-12T10:23:15Z\"",
                "userId=John, songId=1, startTime=\"2016-10-12T10:23:16Z\"",
                "userId=John, songId=1, startTime=\"2016-10-12T10:23:16Z\"",
                "userId=John, songId=1, startTime=\"2016-10-12T10:23:16Z\"",
                "userId=John, songId=2, startTime=\"2016-10-12T10:23:17Z\"",
                "userId=John, songId=2, startTime=\"2016-10-12T10:23:18Z\"",
                "userId=John, songId=2, startTime=\"2016-10-12T10:23:18Z\"",
                "userId=John, songId=3, startTime=\"2016-10-12T10:23:19Z\"",
                "userId=John, songId=3, startTime=\"2016-10-12T10:23:19Z\"",
                "userId=outsideTop3, songId=4, startTime=\"2016-10-12T10:23:21Z\""
        );

        List<SongInformationTuple3> parsedLogs = TestLogParser.parse(logs);

        DataStream<SongInformationTuple3> source = testEnv.fromCollection(parsedLogs)
                .assignTimestampsAndWatermarks(new SongInformationTimestampExtractor());
        List<Tuple2<Long, Long>> top3 = Arrays.asList(
                Tuple2.of(1L, 4L),
                Tuple2.of(2L, 3L),
                Tuple2.of(3L, 2L));

        RankingPerDay<Long> expected20161012 =
                new RankingPerDay<>(LocalDate.parse("2016-10-12"), top3);

        DataStream<RankingPerDay<Long>> processedStream = rankingSongProcessor.processStream(source);

        MatcherBuilder<RankingPerDay<Long>> expectedRecords =
                new ExpectedRecords<RankingPerDay<Long>>()
                        .expect(expected20161012)
                        .refine()
                        .only();

        assertStream(processedStream, expectedRecords);
    }
}