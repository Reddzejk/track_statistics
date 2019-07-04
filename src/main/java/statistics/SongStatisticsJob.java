package statistics;

import base.Flux;
import base.SubFlux;
import base.source.kafka.KafkaConsumerConfig;
import base.source.kafka.KafkaSource;
import converter.SongInformationTuple3;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import statistics.parsing.InitialProcessor;
import statistics.ranking.RankingPerDay;
import statistics.ranking.RankingSongProcessor;
import statistics.ranking.RankingUserProcessor;
import statistics.ranking.top.TopWindowProcessFunction;
import statistics.session.StatisticsProcessor;

import java.time.LocalDate;
import java.util.Collections;
import java.util.Properties;

public
class SongStatisticsJob implements Flux<SongInformationTuple3> {
    private static final int TOP_LENGTH = 10;

    public static void main(String[] args) throws Exception {
        //TODO: Create EnvConfig
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SongStatisticsJob songStatisticsJob = new SongStatisticsJob();

        songStatisticsJob.buildFlux(env);

        env.execute();
    }

    @Override
    public void buildFlux(StreamExecutionEnvironment env) throws Exception {
        DataStream<SongInformationTuple3> sourceStream = this.getSource(env);

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

    @Override
    public DataStream<SongInformationTuple3> getSource(StreamExecutionEnvironment env) {
        KafkaConsumerConfig<String> consumer =
                new KafkaConsumerConfig<>(Collections.singletonList("test"), new KeyedDeserializationSchemaWrapper<>(new SimpleStringSchema()), new Properties());
        KafkaSource<String> kafkaSource = new KafkaSource<>(consumer.createConsumer());
        DataStream<String> source = kafkaSource.getSource(env);
        return new InitialProcessor().processStream(source);
    }


}
