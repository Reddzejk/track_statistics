package statistics.parsing;

import converter.SongInformationTuple3;
import io.flinkspector.core.collection.ExpectedRecords;
import io.flinkspector.core.collection.MatcherBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import statistics.testutils.DataStreamTestBaseJUnit5;

import java.util.Arrays;
import java.util.List;


class InitialProcessorTest extends DataStreamTestBaseJUnit5 {
    private InitialProcessor initialProcessor;

    @BeforeEach
    void setUp() {
        initialProcessor = new InitialProcessor();
    }

    @Test
    void shouldCorrectParseAllLogs() {
        String logWithDate = "userId=John, songId=5, startTime=\"2016-10-12T10:23:13Z\"";
        String logWithTimestamp = "userId=John, songId=5, startTime=1544441730000";
        List<String> logs = Arrays.asList(
                logWithDate,
                logWithTimestamp
        );

        DataStream<String> source = testEnv.fromCollection(logs);
        DataStream<SongInformationTuple3> processedStream = initialProcessor.processStream(source);

        MatcherBuilder<SongInformationTuple3> expected = new ExpectedRecords<SongInformationTuple3>()
                .expect(SongInformationTuple3.parse(logWithDate))
                .expect(SongInformationTuple3.parse(logWithTimestamp))
                .refine()
                .only();

        assertStream(processedStream, expected);
    }

    @Test
    void shouldRejectIncorrectLogAndParseCorrectLog() {
        String logWithDate = "userId=John, songId=5, startTime=\"2016-10-12T10:23:13Z\"";
        String incorrectLog = "incorrectLog";
        List<String> logs = Arrays.asList(
                logWithDate,
                incorrectLog
        );

        DataStream<String> source = testEnv.fromCollection(logs);
        DataStream<SongInformationTuple3> processedStream = initialProcessor.processStream(source);

        MatcherBuilder<SongInformationTuple3> expected = new ExpectedRecords<SongInformationTuple3>()
                .expect(SongInformationTuple3.parse(logWithDate))
                .refine()
                .only();

        assertStream(processedStream, expected);
    }

}