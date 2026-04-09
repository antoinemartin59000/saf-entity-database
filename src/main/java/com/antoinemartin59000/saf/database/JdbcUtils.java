package com.antoinemartin59000.saf.database;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringJoiner;

import com.antoinemartin59000.saf.entity.Pair;

public final class JdbcUtils {

    private JdbcUtils() {

    }

    public static void setLong(PreparedStatement pst, int index, Long value) throws SQLException {
        if (value == null) {
            pst.setNull(index, java.sql.Types.NUMERIC);
        } else {
            pst.setLong(index, value);
        }
    }

    public static void setInt(PreparedStatement pst, int index, Integer value) throws SQLException {
        if (value == null) {
            pst.setNull(index, java.sql.Types.NUMERIC);
        } else {
            pst.setInt(index, value);
        }
    }

    public static void setString(PreparedStatement pst, int index, String value) throws SQLException {
        if (value == null || value.isEmpty()) {
            pst.setNull(index, java.sql.Types.VARCHAR);
        } else {
            pst.setString(index, value);
        }
    }

    public static void setOffsetDateTime(PreparedStatement pst, int index, OffsetDateTime offsetDateTime) throws SQLException {
        if (offsetDateTime == null) {
            pst.setNull(index, java.sql.Types.TIMESTAMP);
        } else {
            pst.setObject(index, offsetDateTime);
        }
    }

    public static void setBigDecimal(PreparedStatement pst, int index, BigDecimal value) throws SQLException {
        if (value == null || value.compareTo(new BigDecimal(Long.MAX_VALUE)) > 0) {
            pst.setNull(index, java.sql.Types.DECIMAL);
        } else {
            pst.setBigDecimal(index, value);
        }
    }

    public static String getString(ResultSet rs, String column) throws SQLException {
        return rs.getString(column);
    }

    public static BigDecimal getBigDecimal(ResultSet rs, String column) throws SQLException {
        BigDecimal value = rs.getBigDecimal(column);
        if (value == null) {
            value = BigDecimal.ZERO;
        }
        return value;
    }

    public static OffsetDateTime getOffsetDateTime(ResultSet rs, String column) throws SQLException {
        OffsetDateTime result = rs.getObject(column, OffsetDateTime.class);
        if (rs.wasNull() || result == null) {
            return null;
        }
        return result;
    }

    public static Long getLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        if (rs.wasNull()) {
            return null;
        }
        return Long.valueOf(value);
    }

    public static Integer getInteger(ResultSet rs, String columnName) throws SQLException {
        int value = rs.getInt(columnName);
        if (rs.wasNull()) {
            return null;
        }
        return Integer.valueOf(value);
    }

    public static void setInteger(PreparedStatement ps, int index, Integer value) throws SQLException {
        if (value == null) {
            ps.setNull(index, java.sql.Types.NUMERIC);
        } else {
            ps.setInt(index, value);
        }
    }

    public static Double getDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        if (rs.wasNull()) {
            return null;
        }
        return Double.valueOf(value);
    }

    public static void setDouble(PreparedStatement ps, int index, Double value) throws SQLException {
        if (value == null) {
            ps.setNull(index, java.sql.Types.NUMERIC);
        } else {
            ps.setDouble(index, value);
        }
    }

    public static Boolean getBoolean(ResultSet rs, String columnName) throws SQLException {
        boolean value = rs.getBoolean(columnName);
        if (rs.wasNull()) {
            return null;
        }
        return Boolean.valueOf(value);
    }

    public static void setBoolean(PreparedStatement pst, int index, Boolean value) throws SQLException {
        if (value == null) {
            // java.sql.Types.BOOLEAN handled only from Oracle 12.2 (12.1 is used at the time I am writing this)
            pst.setNull(index, java.sql.Types.BOOLEAN);
        } else {
            pst.setBoolean(index, value);
        }
    }

    /**
     *
     * @throws IllegalArgumentException is forwarded from Enum.valueOf if the value of the column does not match any
     *             value of the specified enum, or the specified class object does not represent an enum type
     */
    public static <T extends Enum<T>> T getEnum(ResultSet rs, int columnIndex, Class<T> enumType) throws SQLException {
        String value = rs.getString(columnIndex);
        if (rs.wasNull()) {
            return null;
        }
        return Enum.valueOf(enumType, value);
    }

    /**
     *
     * @throws IllegalArgumentException is forwarded from Enum.valueOf if the value of the column does not match any
     *             value of the specified enum, or the specified class object does not represent an enum type
     */
    public static <T extends Enum<T>> T getEnum(ResultSet rs, String columnName, Class<T> enumType) throws SQLException {
        String value = rs.getString(columnName);
        if (rs.wasNull()) {
            return null;
        }
        return Enum.valueOf(enumType, value);
    }

    public static <T extends Enum<?>> void setEnum(PreparedStatement ps, int index, T value) throws SQLException {
        if (value == null) {
            ps.setNull(index, java.sql.Types.VARCHAR);
        } else {
            ps.setString(index, value.name());
        }
    }

    ////////////

    public static enum SortingOrder {
        ASC, DESC;
    }

    /**
     * <pre>
     * Usage:
     *
     * {@code
     * String selectAll = "SELECT" +
     *         "  ID," +
     *         "  CREATEDDATE," +
     *         "  DESCRIPTION," +
     *         "  SPORTCODE," +
     *         "  MARKETTYPEID" +
     *         "FROM" +
     *         "  SCHEMA.MARKETSTYLETEMPLATES";
     *
     * LinkedHashMap<String, Collection<?>> includingValues = new LinkedHashMap<>();
     * linkedHashMap.put("CREATEDDATE", Collections.singleton(Pair.of(dateFrom, dateTo)));
     * linkedHashMap.put("SPORTCODE", Arrays.asList(SportCode.FOOT, SportCode.TENN));
     * linkedHashMap.put("MARKETTYPEID", Arrays.asList(2L, null));
     *
     * LinkedHashMap<String, Collection<?>> excludingValues = new LinkedHashMap<>();
     * linkedHashMap.put("ID", Arrays.asList(37L));
     *
     * PreparedStatement ps = JdbcTools.generateSearchPreparedStatement(dbSession, sql, includingValues);
     * }
     *
     * Now, ps is a PreparedStatement of the following query:
     *      SELECT
     *          ID,
     *          DESCRIPTION,
     *          SPORTCODE,
     *          MARKETTYPEID,
     *      FROM
     *          SCHEMA.MARKETSTYLETEMPLATES
     *      WHERE
     *          1 = 1
     *          AND ((CREATEDDATE >= ? AND CREATEDDATE <= ?))
     *          AND (SPORTCODE IN (?, ?))
     *          AND (MARKETTYPEID IN (?) OR MARKETTYPEID IS NULL)
     *          AND (ID NOT IN (?))
     *
     * and with the parameters designated in the right order and the right type:
     *      ps.setTimestamp(1, new Timestamp(dateFrom.getTime()));
     *      ps.setTimestamp(2, new Timestamp(dateTo.getTime()));
     *      ps.setString(3, SportCode.FOOT.toString());
     *      ps.setString(4, SportCode.TENN.toString());
     *      ps.setLong(5, 2L);
     *      ps.setLong(6, 37L);
     *
     * </pre>
     *
     * @param dbSession A database session
     * @param statement A simple 'SELECT', 'DELETE' or 'UPDATE' query without 'WHERE' clause. e.g:
     *
     *                  <pre>
     *                          {@code
     *                              SELECT
     *                                  ID,
     *                                  DESCRIPTION,
     *                                  SPORTCODE,
     *                                  MARKETTYPEID
     *                              FROM
     *                                  SCHEMA.TEMPLATES
     *                             }
     *                  </pre>
     *
     *                  OR
     *
     *                  <pre>
     *                          {@code
     *                              DELETE FROM SCHEMA.TEMPLATES
     *                             }
     *                  </pre>
     *
     * @param includingValues Mapping the column names with the list of accepted values.<br/>
     *                        The choice of a LinkedHashMap is only for performance purpose.
     * @param excludingValues Mapping the column names with the list of unaccepted values.<br/>
     *                        The choice of a LinkedHashMap is only for performance purpose.
     * @param columnsToOrderBy the sorted list or columns the query should be ordered by
     * @param asc used only if columnsToOrderBy is not empty. True to order in ascending order, false in descending
     * @return a PreparedStatement
     */
    public static PreparedStatement generatePreparedStatement(
            Connection connection,
            String statement,
            LinkedHashMap<String, Collection<?>> includingValues,
            LinkedHashMap<String, Collection<?>> excludingValues,
            String additionalWhereClause, // FIXME remove
            List<Pair<String, SortingOrder>> columnsToOrderBy)
            throws SQLException {

        StringBuilder sql = new StringBuilder(statement).append(" WHERE 1 = 1").append(isEmpty(additionalWhereClause) ? "" : " AND (" + additionalWhereClause + ")");

        for (Entry<String, Collection<?>> entry : includingValues.entrySet()) {
            String condition = generateClauseWithPlaceHolders(entry.getKey(), entry.getValue(), true);
            sql.append(condition);
        }

        for (Entry<String, Collection<?>> entry : excludingValues.entrySet()) {
            String condition = generateClauseWithPlaceHolders(entry.getKey(), entry.getValue(), false);
            sql.append(condition);
        }

        StringJoiner orderByJoiner = new StringJoiner(", ", " ORDER BY ", "").setEmptyValue("");
        columnsToOrderBy.forEach(columnToOrderBy -> orderByJoiner.add(columnToOrderBy.first() + " " + columnToOrderBy.second()));
        sql.append(orderByJoiner.toString());

        PreparedStatement ps = connection.prepareStatement(sql.toString());

        int i = 0;
        for (Collection<?> values : includingValues.values()) { // LinkedHashMap ensures we will follow the same order as the first loop
            for (Object value : values) {
                if (value == null) {
                    // the "IS NULL" condition is handled in generateInClauseWithPlaceHolders()
                } else if (value.getClass() == Pair.class) {
                    Pair<?, ?> pair = (Pair<?, ?>) value;
                    set(ps, ++i, pair.first());
                    set(ps, ++i, pair.second());
                } else {
                    set(ps, ++i, value);
                }
            }
        }

        for (Collection<?> values : excludingValues.values()) { // LinkedHashMap ensures we will follow the same order as the first loop
            for (Object value : values) {
                if (value == null) {
                    // the "IS NOT NULL" condition is handled in generateInClauseWithPlaceHolders()
                } else if (value.getClass() == Pair.class) {
                    Pair<?, ?> pair = (Pair<?, ?>) value;
                    set(ps, ++i, pair.first());
                    set(ps, ++i, pair.second());
                } else {
                    set(ps, ++i, value);
                }
            }
        }

        return ps;
    }

    public static void set(PreparedStatement pst, int index, Object value) throws SQLException {

        if (value == null) {
            pst.setNull(index, java.sql.Types.NUMERIC);
        } else if (value instanceof Long longValue) {
            setLong(pst, index, longValue);
        } else if (value instanceof String string) {
            setString(pst, index, string);
        } else if (value instanceof Integer integer) {
            setInt(pst, index, integer);
        } else if (value instanceof BigDecimal bigDecimal) {
            setBigDecimal(pst, index, bigDecimal);
        } else if (value instanceof Boolean bool) {
            setBoolean(pst, index, bool);
        } else if (value instanceof OffsetDateTime offsetDateTime) {
            setOffsetDateTime(pst, index, offsetDateTime);
        } else if (value instanceof Double doubleValue) {
            setDouble(pst, index, doubleValue);
        } else if (value instanceof Enum<?>) {
            setEnum(pst, index, (Enum<?>) value);
        } else {
            // FIXME change runtime
            throw new RuntimeException("Invalid type for JdbcTools set method - " + value.getClass());
        }
    }

    /**
     * <pre>
     * JdbcTools.generateClauseWithPlaceHolders("SPORTCODE", ["FOOT", "BASK"], true)                    => " AND (SPORTCODE IN (?, ?))"
     * JdbcTools.generateClauseWithPlaceHolders("SPORTCODE", ["FOOT", null], true)                      => " AND (SPORTCODE IN (?) OR SPORTCODE IS NULL)"
     * JdbcTools.generateClauseWithPlaceHolders("SPORTCODE", [null], true)                              => " AND (SPORTCODE IS NULL)"
     * JdbcTools.generateClauseWithPlaceHolders("SPORTCODE", [], true)                                  => ""
     *
     * JdbcTools.generateClauseWithPlaceHolders("SPORTCODE", ["FOOT", "BASK"], false)                   => " AND (SPORTCODE IS NULL OR SPORTCODE NOT IN (?, ?))"
     * JdbcTools.generateClauseWithPlaceHolders("SPORTCODE", ["FOOT", null], false)                     => " AND (SPORTCODE NOT IN (?) AND SPORTCODE IS NOT NULL)"
     * JdbcTools.generateClauseWithPlaceHolders("SPORTCODE", [null], false)                             => " AND (SPORTCODE IS NOT NULL)"
     * JdbcTools.generateClauseWithPlaceHolders("SPORTCODE", [], false)                                 => ""
     *
     * JdbcTools.generateClauseWithPlaceHolders("CUTOFFTIME", [Pair.of(dateFrom, dateTo)], true)        => " AND (CUTOFFTIME >= ? AND CUTOFFTIME <= ?)"
     * JdbcTools.generateClauseWithPlaceHolders("CUTOFFTIME", [Pair.of(dateFrom, dateTo), null], true)  => " AND ((CUTOFFTIME >= ? AND CUTOFFTIME <= ?) OR CUTOFFTIME IS NULL)"
     *
     * JdbcTools.generateClauseWithPlaceHolders("CUTOFFTIME", [Pair.of(dateFrom, dateTo)], false)       => " AND (CUTOFFTIME < ? AND CUTOFFTIME > ?)"
     *
     * </pre>
     */
    public static String generateClauseWithPlaceHolders(String columnName, Collection<?> values, boolean including) {
        if (values.isEmpty()) {
            return "";
        }

        StringJoiner intervalJoiner = new StringJoiner(including ? " OR " : " AND ").setEmptyValue("");
        StringJoiner inJoiner = new StringJoiner(",", columnName + (including ? " " : " NOT ") + "IN (", ")").setEmptyValue("");
        boolean containsNull = false;
        for (Object o : values) {
            if (o == null) {
                containsNull = true; // we will add a "columnName IS [NOT] NULL" clause later
            } else if (o.getClass() == Pair.class) {
                StringBuilder intervalSb = new StringBuilder();
                intervalSb
                        .append(including ? "" : "NOT ")
                        .append("(")
                        .append(columnName).append(" >= ").append("?")
                        .append(" AND ")
                        .append(columnName).append(" <= ").append("?");
                intervalSb.append(")");
                intervalJoiner.add(intervalSb);
            } else {
                inJoiner.add("?");
            }
        }

        String includingNullIfNotExcluded = !including && !containsNull ? columnName + " IS NULL OR " : "";
        StringJoiner joiner = new StringJoiner(including ? " OR " : " AND ", " AND (" + includingNullIfNotExcluded, ")").setEmptyValue("");

        if (intervalJoiner.length() > 0) {
            joiner.add(intervalJoiner.toString());
        }

        if (inJoiner.length() > 0) {
            joiner.add(inJoiner.toString());
        }

        if (containsNull) {
            StringBuilder nullClause = new StringBuilder().append(columnName).append(" IS").append(including ? " " : " NOT ").append("NULL");
            joiner.add(nullClause);
        }

        return joiner.toString();
    }

    public static boolean isEmpty(String string) {
        return string == null || string.isEmpty();
    }

}
