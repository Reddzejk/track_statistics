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
import java.util.Collections;
import java.util.List;

class RankingUserProcessorTest extends DataStreamTestBaseJUnit5 {
    private RankingUserProcessor rankingUserProcessor;
    private final int TOP_THREE = 3;

    @BeforeEach
    void setUp() {
        rankingUserProcessor = new RankingUserProcessor(new TopWindowProcessFunction<>(TOP_THREE));
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


        RankingPerDay<String> expected20161012 =
                new RankingPerDay<>(LocalDate.parse("2016-10-12"), Collections.singletonList(Tuple2.of("John", 3L)));
        RankingPerDay<String> expected20161013 =
                new RankingPerDay<>(LocalDate.parse("2016-10-13"), Arrays.asList(Tuple2.of("Eva", 2L), Tuple2.of("John", 1L)));

        DataStream<RankingPerDay<String>> processedStream = rankingUserProcessor.processStream(source);

        MatcherBuilder<RankingPerDay<String>> expectedRecords =
                new ExpectedRecords<RankingPerDay<String>>()
                        .expect(expected20161012)
                        .expect(expected20161013)
                        .refine()
                        .only();

        assertStream(processedStream, expectedRecords);
    }

    @Test
    void shouldReturn10RecordsWithTopUsersPerDayAndIgnoreOtherUsers() {
        List<String> logs = Arrays.asList(
                "userId=John1, songId=1, startTime=\"2016-10-12T10:23:15Z\"",
                "userId=John1, songId=2, startTime=\"2016-10-12T10:23:16Z\"",
                "userId=John1, songId=3, startTime=\"2016-10-12T10:23:16Z\"",
                "userId=John1, songId=4, startTime=\"2016-10-12T10:23:16Z\"",
                "userId=Eva2, songId=1, startTime=\"2016-10-12T10:23:17Z\"",
                "userId=Eva2, songId=2, startTime=\"2016-10-12T10:23:18Z\"",
                "userId=Eva2, songId=3, startTime=\"2016-10-12T10:23:18Z\"",
                "userId=Ed3, songId=1, startTime=\"2016-10-12T10:28:19Z\"",
                "userId=Ed3, songId=2, startTime=\"2016-10-12T10:28:19Z\"",
                "userId=outsideTop3, songId=100, startTime=\"2016-10-12T10:23:21Z\""
        );

        List<SongInformationTuple3> parsedLogs = TestLogParser.parse(logs);

        DataStream<SongInformationTuple3> source = testEnv.fromCollection(parsedLogs)
                .assignTimestampsAndWatermarks(new SongInformationTimestampExtractor());
        List<Tuple2<String, Long>> top3 = Arrays.asList(
                Tuple2.of("John1", 4L),
                Tuple2.of("Eva2", 3L),
                Tuple2.of("Ed3", 2L));

        RankingPerDay<String> expected20161012 =
                new RankingPerDay<>(LocalDate.parse("2016-10-12"), top3);

        DataStream<RankingPerDay<String>> processedStream = rankingUserProcessor.processStream(source);

        MatcherBuilder<RankingPerDay<String>> expectedRecords =
                new ExpectedRecords<RankingPerDay<String>>()
                        .expect(expected20161012)
                        .refine()
                        .only();

        assertStream(processedStream, expectedRecords);
    }
}