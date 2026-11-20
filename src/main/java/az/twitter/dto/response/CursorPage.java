package az.twitter.dto.response;

import java.util.List;
import java.util.function.Function;

/**
 * Keyset (cursor) page. {@code nextCursor} is null on the last page; ids are strings so JS clients don't lose precision.
 */
public record CursorPage<T>(List<T> items, String nextCursor) {

    /**
     * @param rows   up to {@code limit + 1} rows; the extra row only signals that another page exists
     * @param cursor extracts the keyset value of a row
     */
    public static <R, T> CursorPage<T> of(List<R> rows, int limit, Function<R, Long> cursor, Function<R, T> mapper) {
        boolean hasNext = rows.size() > limit;
        List<R> page = hasNext ? rows.subList(0, limit) : rows;
        String next = hasNext ? String.valueOf(cursor.apply(page.getLast())) : null;
        return new CursorPage<>(page.stream().map(mapper).toList(), next);
    }
}
