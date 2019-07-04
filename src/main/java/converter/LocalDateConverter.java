package converter;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

public final class LocalDateConverter {
    private LocalDateConverter() {
    }

    public static LocalDate toLocalDate(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC).toLocalDate();
    }
}
