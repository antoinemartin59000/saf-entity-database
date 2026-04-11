package com.antoinemartin59000.saf.database;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;

import com.antoinemartin59000.saf.entity.Inflector;
import com.antoinemartin59000.saf.entity.Pair;
import com.antoinemartin59000.saf.entity.SafEntity;
import com.antoinemartin59000.saf.entity.SafEntitySearch;

public class SafEntityDatabase<T extends SafEntity, S extends SafEntitySearch> {

    @FunctionalInterface
    private interface Setter<T, U> {

        void accept(T t, U u) throws SQLException;

    }

    private final Class<T> safEntityClass;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Setter<T, ResultSet>> setters;
    private final Map<String, Function<S, Collection<?>>> includingValuesGetter;
    private final List<Pair<String, Function<T, ?>>> upsertColumns;

    public SafEntityDatabase(Class<T> safEntityClass) {
        this.safEntityClass = safEntityClass;
        this.tableName = getPluralSnakeCaseName(safEntityClass);
        this.columnNames = getFieldNamesInSnakeCase(safEntityClass);
        this.setters = getSetters();
        this.includingValuesGetter = buildSearchMap(safEntityClass);
        this.upsertColumns = buildUpsertGetters(safEntityClass);
    }

    public List<T> loadAll(Connection connection) {

        StringJoiner columns = new StringJoiner(", ", "", "").setEmptyValue("");
        columnNames.forEach(columns::add);
        String sql = "select " + columns + " from " + tableName;

        try (PreparedStatement pst = connection.prepareStatement(sql)) {

            try (ResultSet rs = pst.executeQuery()) {

                List<T> result = new ArrayList<>();
                while (rs.next()) {
                    T txStatus = build(rs);
                    result.add(txStatus);
                }

                return result;
            }

        } catch (SQLException e) {
            throw new RuntimeException("TODO - table: " + tableName, e);
        }
    }

    public List<T> search(Connection connection, S search) {

        StringJoiner columns = new StringJoiner(", ", "", "").setEmptyValue("");
        columnNames.forEach(columns::add);
        String sql = "select " + columns + " from " + tableName;

        LinkedHashMap<String, Collection<?>> includingValues = new LinkedHashMap<>();
        for (Map.Entry<String, Function<S, Collection<?>>> entry : includingValuesGetter.entrySet()) {
            String columnName = entry.getKey();
            Function<S, Collection<?>> getter = entry.getValue();

            Collection<?> includingValue = getter.apply(search);
            if (includingValue == null) { // hack to handle sub param query (done in Utils used by EntityCache too)
                return Collections.emptyList();
            }
            includingValues.put(columnName, includingValue);
        }

        LinkedHashMap<String, Collection<?>> excludingValues = new LinkedHashMap<>();

        Pair<String, JdbcUtils.SortingOrder> sortById = Pair.of("ID", JdbcUtils.SortingOrder.ASC);
        List<Pair<String, JdbcUtils.SortingOrder>> orderBy = Arrays.asList(sortById);

        return search(connection, sql, includingValues, excludingValues, search.getCustomWhereClause(), orderBy);
    }

    public T search(Connection connection, Long id) {
        StringJoiner columns = new StringJoiner(", ", "", "").setEmptyValue("");
        columnNames.forEach(columns::add);
        String sql = "select " + columns + " from " + tableName;

        LinkedHashMap<String, Collection<?>> includingValues = new LinkedHashMap<>();
        includingValues.put("id", List.of(id));
        LinkedHashMap<String, Collection<?>> excludingValues = new LinkedHashMap<>();

        return search(connection, sql, includingValues, excludingValues, null, List.of()).stream().findAny().orElse(null);
    }

    private List<T> search(
            Connection connection,
            String sql,
            LinkedHashMap<String, Collection<?>> includingValues,
            LinkedHashMap<String, Collection<?>> excludingValues,
            String additionalWhereClause, // FIXME remove
            List<Pair<String, JdbcUtils.SortingOrder>> columnsToOrderBy) {

        try (PreparedStatement pst = JdbcUtils.generatePreparedStatement(connection, sql, includingValues, excludingValues, additionalWhereClause, columnsToOrderBy)) {

            try (ResultSet rs = pst.executeQuery()) {

                List<T> result = new ArrayList<>();
                while (rs.next()) {
                    T txStatus = build(rs);
                    result.add(txStatus);
                }

                return result;
            }

        } catch (SQLException e) {
            throw new RuntimeException("TODO - " + sql, e);
        }
    }

    private T build(ResultSet rs) throws SQLException {

        T entity;
        try {
            entity = (T) safEntityClass.getConstructors()[0].newInstance();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SecurityException e) {
            throw new RuntimeException("TODO - table: " + tableName, e);
        }

        entity.setId(JdbcUtils.getLong(rs, "id"));
        for (Setter<T, ResultSet> setter : setters) {
            setter.accept(entity, rs);
        }

        return entity;
    }

    public Long insert(Connection connection, T entity) {

        StringJoiner columns = new StringJoiner(", ", ", ", "").setEmptyValue("");
        upsertColumns.stream().map(Pair::first).forEach(columns::add);

        StringJoiner questionMarks = new StringJoiner(", ", ", ", "").setEmptyValue("");
        upsertColumns.stream().map(i -> "?").forEach(questionMarks::add);

        String sql = "insert into " + tableName + "(ID" + columns + ") VALUES(nextval('seq_" + tableName + "')" + questionMarks + ") RETURNING ID";

        try (PreparedStatement pst = connection.prepareStatement(sql)) {

            int i = 1;
            for (Pair<String, Function<T, ?>> pair : upsertColumns) {
                Function<T, ?> getter = pair.second();

                JdbcUtils.set(pst, i++, getter.apply(entity));
            }

            try (ResultSet rs = pst.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("id");
                } else {
                    throw new RuntimeException("TODO - table: " + tableName);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("TODO - table: " + tableName, e);
        }
    }

    public void update(Connection connection, T entity) {

        StringJoiner columns = new StringJoiner(" = ?, ", "", " = ?").setEmptyValue("");
        upsertColumns.stream().map(Pair::first).forEach(columns::add);

        String sql = "update " + tableName + " SET " + columns + " WHERE ID = ?";

        try (PreparedStatement pst = connection.prepareStatement(sql)) {

            int i = 1;
            for (Pair<String, Function<T, ?>> pair : upsertColumns) {
                Function<T, ?> getter = pair.second();
                JdbcUtils.set(pst, i++, getter.apply(entity));
            }

            JdbcUtils.setLong(pst, i++, entity.getId());

            pst.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("TODO - " + this.tableName, e);
        }

    }

    public void delete(Connection connection, Long id) {
        // TODO check if batching a single delete makes a difference worth considering
        delete(connection, Arrays.asList(id));
    }

    public void delete(Connection connection, Iterable<Long> ids) {

        String sql = "DELETE FROM " + tableName + " where id = ?";

        try (PreparedStatement pst = connection.prepareStatement(sql)) {

            for (Long id : ids) {
                JdbcUtils.setLong(pst, 1, id);
                pst.addBatch();
            }

            pst.executeBatch();

        } catch (SQLException e) {
            throw new RuntimeException("TODO - table: " + tableName, e);
        }

    }

    private String getPluralSnakeCaseName(Class<?> clazz) {
        String className = clazz.getSimpleName();
        String snake = toSnakeCase(className);
        return Inflector.getInstance().pluralize(snake);
    }

    private List<String> getFieldNamesInSnakeCase(Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        List<String> result = new ArrayList<>();
        result.add("id");

        for (Field field : fields) {
            String name = field.getName();
            String snakeCase = toSnakeCase(name);
            result.add(snakeCase);
        }

        return result;
    }

    private List<Setter<T, ResultSet>> getSetters() {
        List<Setter<T, ResultSet>> result = new ArrayList<>();

        result.add((i, rs) -> JdbcUtils.getLong(rs, "id"));

        for (Field field : safEntityClass.getDeclaredFields()) {
            String fieldName = field.getName();
            String snakeCase = toSnakeCase(fieldName);
            String setterName = "set" + capitalize(fieldName);

            Method setter;
            try {
                setter = safEntityClass.getMethod(setterName, field.getType());
            } catch (NoSuchMethodException | SecurityException e) {
                throw new RuntimeException("Failed to create setter for: " + fieldName + " for class " + safEntityClass, e);
            }

            result.add((i, rs) -> {
                try {
                    Object value;

                    Class<?> fieldType = field.getType();
                    if (fieldType == String.class) {
                        value = JdbcUtils.getString(rs, snakeCase);
                    } else if (fieldType == int.class || fieldType == Integer.class) {
                        value = JdbcUtils.getInteger(rs, snakeCase);
                    } else if (fieldType == long.class || fieldType == Long.class) {
                        value = JdbcUtils.getLong(rs, snakeCase);
                    } else if (fieldType == BigDecimal.class) {
                        value = JdbcUtils.getBigDecimal(rs, snakeCase);
                    } else if (fieldType == boolean.class || fieldType == Boolean.class) {
                        value = JdbcUtils.getBoolean(rs, snakeCase);
                    } else if (fieldType == double.class || fieldType == Double.class) {
                        value = JdbcUtils.getDouble(rs, snakeCase);
                    } else if (fieldType == OffsetDateTime.class) {
                        value = JdbcUtils.getOffsetDateTime(rs, snakeCase);
                    } else if (fieldType.isEnum()) {
                        value = JdbcUtils.getEnum(rs, snakeCase, (Class<? extends Enum>) fieldType);
                    } else {
                        throw new UnsupportedOperationException(fieldType + " not supported");
                    }

                    setter.invoke(i, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException("Failed to create setter for: " + fieldName, e);
                }
            });

        }

        return result;
    }

    private Map<String, Function<S, Collection<?>>> buildSearchMap(Class<?> entityClass) { // FIXME base searchMap only on EntitySearch class
        Map<String, Function<S, Collection<?>>> resultMap = new LinkedHashMap<>();

        resultMap.put("id", s -> s.getIds());

        String entitySimpleName = entityClass.getSimpleName();
        String searchClassName = entityClass.getPackage().getName() + "." + entitySimpleName + "Search";

        Class<?> searchClassResolved;
        try {
            searchClassResolved = Class.forName(searchClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Search class not found for: " + entitySimpleName, e);
        }

        for (Field field : entityClass.getDeclaredFields()) {
            String fieldName = field.getName();
            String snakeCase = toSnakeCase(fieldName);

            // Construct expected getter name
            // Example: for "conventionalVarchar" -> "getConventionalVarchars"
            String getterName;
            if (field.getType() == OffsetDateTime.class) {
                getterName = "get" + capitalize(fieldName) + "Intervals";
            } else {
                getterName = "get" + capitalize(Inflector.getInstance().pluralize(fieldName));
            }

            Method method;
            try {
                method = searchClassResolved.getMethod(getterName);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Method " + getterName + " not found for class " + searchClassResolved, e);
            }

            if (!Collection.class.isAssignableFrom(method.getReturnType())) {
                throw new RuntimeException(method.getName() + " must return a Collection");
            }

            Function<S, Collection<?>> func = s -> {
                try {
                    return (Collection<?>) method.invoke(s);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to invoke getter " + getterName, e);
                }
            };
            resultMap.put(snakeCase, func);

        }

        return resultMap;
    }

    private List<Pair<String, Function<T, ?>>> buildUpsertGetters(Class<T> clazz) {
        List<Pair<String, Function<T, ?>>> result = new ArrayList<>();

        for (Field field : clazz.getDeclaredFields()) {
            String fieldName = field.getName();
            String snakeCase = toSnakeCase(fieldName);
            String getterName = "get" + capitalize(fieldName);

            try {
                Method getter = clazz.getMethod(getterName);
                Function<T, ?> function = t -> {
                    try {
                        return getter.invoke(t);
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                        throw new RuntimeException("Failed to invoke getter: " + getterName, e);
                    }
                };
                result.add(Pair.of(snakeCase, function));
            } catch (NoSuchMethodException e) {
                throw new UnsupportedOperationException(e);
            }
        }

        return result;
    }

    public static String toSnakeCase(String input) {
        return input.replaceAll("([a-z0-9])([A-Z]+)", "$1_$2").toLowerCase();
    }

    public static String capitalize(String words) {
        if (words == null) {
            return null;
        }
        String result = words.trim();
        if (result.length() == 0) {
            return "";
        }
        if (result.length() == 1) {
            return result.toUpperCase();
        }
        return "" + Character.toUpperCase(result.charAt(0)) + result.substring(1);
    }

}
