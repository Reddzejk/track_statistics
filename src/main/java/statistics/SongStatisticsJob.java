package statistics;

import base.SubFlux;
import base.source.kafka.KafkaConsumerConfig;
import base.source.kafka.KafkaSource;
import converter.SongInformationTuple3;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import statistics.ranking.RankingSongProcessor;
import statistics.ranking.RankingUserProcessor;
import statistics.ranking.top.TopWindowProcessFunction;
import statistics.session.StatisticsProcessor;
import util.RankingPerDay;

import java.time.LocalDate;
import java.util.Collections;
import java.util.Properties;

public class SongStatisticsJob {
    private static final int TOP_LENGTH = 10;


    public static void main(String[] args) throws Exception {
        //TODO: Create EnvConfig
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SongStatisticsJob songStatisticsJob = new SongStatisticsJob();

        DataStream<SongInformationTuple3> initialProcessedStream = getSourceDataStream(env);
        songStatisticsJob.buildProcessWithSinks(initialProcessedStream);

        env.execute();
    }

    private static DataStream<SongInformationTuple3> getSourceDataStream(StreamExecutionEnvironment env) {
        //TODO: Create Props
        KafkaConsumerConfig<String> consumer =
                new KafkaConsumerConfig<>(Collections.singletonList("test"), new SimpleStringSchema(), new Properties());
        KafkaSource<String> kafkaSource = new KafkaSource<>(consumer.createConsumer());
        DataStream<String> source = kafkaSource.getSource(env);
        return new InitialProcessor().processStream(source);
    }


    @VisibleForTesting
    void buildProcessWithSinks(DataStream<SongInformationTuple3> sourceStream) {
  //TODO: ADDOtherSInk
        SubFlux<SongInformationTuple3, RankingPerDay<Long>> topSong
                = new SubFlux<>(sourceStream, new RankingSongProcessor(new TopWindowProcessFunction<>(TOP_LENGTH)), DataStream::printToErr);

        SubFlux<SongInformationTuple3, RankingPerDay<String>> topUser
                = new SubFlux<>(sourceStream, new RankingUserProcessor(new TopWindowProcessFunction<>(TOP_LENGTH)), DataStream::printToErr);

        SubFlux<SongInformationTuple3, Tuple5<LocalDate, Long, Long, Long, Long>> statistics
                = new SubFlux<>(sourceStream, new StatisticsProcessor(), DataStream::printToErr);

        topSong.buildSubFlux();
        topUser.buildSubFlux();
        statistics.buildSubFlux();
    }
}
