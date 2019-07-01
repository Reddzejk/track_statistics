package converter;

import org.apache.flink.api.java.tuple.Tuple3;

public class SongInformationTuple3 extends Tuple3<String, Long, Long> {
    private SongInformationTuple3() {
        super();
    }

    public static SongInformationTuple3 parse(String log) {
        SongInformationTuple3 songInfo = new SongInformationTuple3();
        String[] pairsKeyValue = log.split(",\\s");
        for (String pair : pairsKeyValue) {
            parsePairKeyValue(songInfo, pair);
        }
        return songInfo;
    }

    private static void parsePairKeyValue(SongInformationTuple3 songInfo, String pairKeyValue) {
        String[] split = pairKeyValue.split("=");
        String name = split[0];
        String value = split[1];
        SongInformationConverter.findByName(name).parse(songInfo, value);
    }

    public String getUserId() {
        return f0;
    }

    public Long getSongId() {
        return f1;
    }

    public Long getTimestamp() {
        return f2;
    }
}
