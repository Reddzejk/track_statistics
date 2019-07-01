package converter;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Pretty way to map String to SongInformationTuple3.
 */
public enum SongInformationConverter {
    USER_ID("userId") {
        @Override
        public void parse(SongInformationTuple3 songInformation, String value) {
            songInformation.f0 = value;
        }
    }, SONG_ID("songId") {
        @Override
        public void parse(SongInformationTuple3 songInformation, String value) {
            songInformation.f1 = Long.parseLong(value);
        }
    }, TIMESTAMP("startTime") {
        private final transient DateTimeFormatter dateTimeFormatter = DateTimeFormatter
                .ofPattern("\"yyyy-MM-dd'T'HH:mm:ssz\"")
                .withZone(ZoneOffset.UTC);

        @Override
        public void parse(SongInformationTuple3 songInformation, String value) {
            if (StringUtils.isNumeric(value)) {
                songInformation.f2 = Long.parseLong(value);
            } else {
                LocalDateTime parse = LocalDateTime.parse(value, dateTimeFormatter);
                songInformation.f2 = parse.toInstant(ZoneOffset.UTC).toEpochMilli();
            }
        }
    };

    private static final Map<String, SongInformationConverter> CONVERTERS = new HashMap<>();

    static {
        for (SongInformationConverter converter : SongInformationConverter.values()) {
            CONVERTERS.put(converter.name, converter);
        }
    }

    private String name;

    SongInformationConverter(String name) {
        this.name = name;
    }

    public static SongInformationConverter findByName(String name) {
        return CONVERTERS.get(name);
    }

    public abstract void parse(SongInformationTuple3 songInformation, String value);

}
