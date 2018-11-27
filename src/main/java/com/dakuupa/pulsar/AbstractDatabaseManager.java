package com.dakuupa.pulsar;

import com.dakuupa.pulsar.annotations.DbAutoIncrement;
import com.dakuupa.pulsar.annotations.DbNotNull;
import com.dakuupa.pulsar.annotations.DbPrimaryKey;
import com.dakuupa.pulsar.annotations.DbSize;
import com.dakuupa.pulsar.annotations.DbUnique;
import com.dakuupa.pulsar.typeconverter.AbstractTypeConverter;
import com.dakuupa.pulsar.typeconverter.TypeConverter;
import com.dakuupa.pulsar.typeconverter.mysql.BooleanTypeConverter;
import com.dakuupa.pulsar.typeconverter.mysql.DateTypeConverter;
import com.dakuupa.pulsar.typeconverter.mysql.DoubleTypeConverter;
import com.dakuupa.pulsar.typeconverter.mysql.FloatTypeConverter;
import com.dakuupa.pulsar.typeconverter.mysql.IntegerTypeConverter;
import com.dakuupa.pulsar.typeconverter.mysql.LongTypeConverter;
import com.dakuupa.pulsar.typeconverter.mysql.MySQLTypeConverter;
import com.dakuupa.pulsar.typeconverter.mysql.StringTypeConverter;
import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Base DB Manager Allow types are double, float, long, integer, boolean, and String values.
 *
 * @author EWilliams
 *
 * @param <T> DB class that extends Entity
 */
public abstract class AbstractDatabaseManager<T extends Entity> {

    private static final String NULL_STATEMENT_MSG = "Null statement";
    protected Logger logger;

    private String tableName;
    private Connection dbConnection;
    private Class<T> entityClass;

    private File logFile;
    private boolean verboseLogging;

    private static final HashMap<Class, Class> TYPE_CONVERTERS = new HashMap<>();
    private static final HashMap<String, Class> PRIMITIVE_TYPE_CONVERTERS = new HashMap<>();

    static {
        //initialize default type converters
        TYPE_CONVERTERS.put(String.class, StringTypeConverter.class);
        TYPE_CONVERTERS.put(Integer.class, IntegerTypeConverter.class);
        TYPE_CONVERTERS.put(Double.class, DoubleTypeConverter.class);
        TYPE_CONVERTERS.put(Long.class, LongTypeConverter.class);
        TYPE_CONVERTERS.put(Float.class, FloatTypeConverter.class);
        TYPE_CONVERTERS.put(Boolean.class, BooleanTypeConverter.class);
        TYPE_CONVERTERS.put(Date.class, DateTypeConverter.class);

        //init primitive types also
        PRIMITIVE_TYPE_CONVERTERS.put("int", IntegerTypeConverter.class);
        PRIMITIVE_TYPE_CONVERTERS.put("double", DoubleTypeConverter.class);
        PRIMITIVE_TYPE_CONVERTERS.put("long", LongTypeConverter.class);
        PRIMITIVE_TYPE_CONVERTERS.put("float", FloatTypeConverter.class);
        PRIMITIVE_TYPE_CONVERTERS.put("boolean", BooleanTypeConverter.class);
    }

    public AbstractDatabaseManager(Connection con) {
        init(con, (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
    }

    public AbstractDatabaseManager(Connection dbConnection, File logFile, boolean verboseLogging) {
        this.dbConnection = dbConnection;
        this.logFile = logFile;
        this.verboseLogging = verboseLogging;
        init(dbConnection, (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
    }

    public AbstractDatabaseManager(Connection con, Class<T> entityClass) {
        init(con, entityClass);
    }

    public AbstractDatabaseManager(Connection con, TypeConverter... converters) {
        for (TypeConverter converter : converters) {
            Class<T> clazz = (Class<T>) ((ParameterizedType) converter.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            TYPE_CONVERTERS.put(clazz, converter.getClass());
        }

        init(con, (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
    }

    public AbstractDatabaseManager(Connection con, File logFile, boolean verboseLogging, TypeConverter... converters) {

        this.logFile = logFile;
        this.verboseLogging = verboseLogging;

        for (TypeConverter converter : converters) {
            Class<T> clazz = (Class<T>) ((ParameterizedType) converter.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            TYPE_CONVERTERS.put(clazz, converter.getClass());
        }

        init(con, (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
    }

    private void init(Connection con, Class<T> entityClass) {

        System.setProperty("java.util.logging.SimpleFormatter.format", "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS.%1$tL %4$-7s [%3$s] (%2$s) %5$s %6$s%n");
        logger = Logger.getLogger(this.getClass().getCanonicalName());

        if (verboseLogging) {
            ConsoleHandler handler = new ConsoleHandler();
            handler.setLevel(Level.FINEST);
            logger.setLevel(Level.FINEST);
            logger.addHandler(handler);
        }

        if (logFile != null && logFile.exists()) {

            try {
                FileHandler fh = new FileHandler(logFile.getAbsolutePath());
                SimpleFormatter formatter = new SimpleFormatter();
                fh.setFormatter(formatter);
                logger.addHandler(fh);
            } catch (IOException | SecurityException ex) {
                Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        dbConnection = con;

        this.entityClass = entityClass;
        this.tableName = entityClass.getSimpleName().toLowerCase();

        String tableNameAnno = ReflectUtil.tableName(entityClass);
        if (tableNameAnno != null) {
            this.tableName = tableNameAnno.toLowerCase();
        }

        for (Class key : TYPE_CONVERTERS.keySet()) {
            logger.log(Level.INFO, "Type Converter {0}", key.getCanonicalName());
        }
        for (String key : PRIMITIVE_TYPE_CONVERTERS.keySet()) {
            logger.log(Level.INFO, "Primitive Type Converter {0}", key);
        }

        if (dbConnection != null) {
            setupTable();
        }
    }

    private void setupTable() {

        Map<String, Column> dbColumns = new HashMap<>();
        Map<String, Column> entityColumns = new HashMap<>();

        List<String> dbColumnNames = new ArrayList<>();
        List<String> removedColumns = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        List<Field> addedColumns = new ArrayList<>();

        try {

            if (!tableExists()) {
                String query = getCreateQuery();
                logger.log(Level.INFO, "Creating table {0} using query: {1}", new Object[]{tableName, query});

                try (Statement statement = getStatement()) {
                    if (statement != null) {
                        statement.execute(query);
                    } else {
                        logger.info(NULL_STATEMENT_MSG);
                    }
                }

            } else {

                //compare for changes
                //load DB column info
                try (Statement statement = getStatement()) {
                    if (statement != null) {
                        try (ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName)) {
                            while (rs.next()) {
                                Column col = new Column(rs);
                                dbColumns.put(col.getName(), col);
                                dbColumnNames.add(col.getName());
                            }
                        }

                    } else {
                        logger.info("Null statement!");
                    }
                }

                //load Entity column info
                T entityObj = getNewInstanceOfEntity();

                if (entityObj != null) {

                    Class clazz = entityObj.getClass();

                    if (clazz != null) {

                        List<Field> fields = ReflectUtil.getAllFields(clazz);
                        for (Field field : fields) {

                            field.setAccessible(true);
                            if (ReflectUtil.fieldIsOkForDatabase(field)) {

                                Column col = new Column(field, this);
                                entityColumns.put(col.getName(), col);

                                fieldNames.add(ReflectUtil.getColumnName(field));

                                if (!dbColumnNames.contains(ReflectUtil.getColumnName(field))) {
                                    addedColumns.add(field);
                                }
                            }

                        }
                    } else {
                        logger.info("Null entity!");
                    }
                } else {
                    logger.info("Null entity!");
                }

                //add new columns
                for (Field added : addedColumns) {
                    String addQuery = getAddColumnQuery(added);
                    if (addQuery != null && !addQuery.isEmpty()) {
                        logger.log(Level.INFO, "Add column query: {0}", addQuery);

                        Column col = new Column(added, this);
                        dbColumns.put(col.getName(), col);

                        try (Statement statement = getStatement()) {
                            if (statement != null) {
                                statement.execute(addQuery);

                            } else {
                                logger.info(NULL_STATEMENT_MSG);
                            }
                        }

                    }
                }

                //remove old columns
                for (String col : dbColumnNames) {
                    if (!fieldNames.contains(col)) {
                        removedColumns.add(col);
                    }
                }
                if (!removedColumns.isEmpty()) {
                    for (String col : removedColumns) {
                        String dropQuery = "ALTER TABLE `" + tableName + "` DROP COLUMN " + col + ";";
                        logger.log(Level.INFO, "Remove column query: {0}", dropQuery);
                        try (Statement statement = getStatement()) {
                            if (statement != null) {
                                statement.execute(dropQuery);

                            } else {
                                logger.info(NULL_STATEMENT_MSG);
                            }
                        }
                        dbColumns.remove(col);
                    }
                }

                //modify existing columns
                for (Map.Entry<String, Column> entry : entityColumns.entrySet()) {
                    String columnName = entry.getKey();
                    Column entityColumn = entry.getValue();
                    Column dbColumn = dbColumns.get(columnName);

                    logger.log(Level.INFO, dbColumn.toString());

                    if (!entityColumn.equals(dbColumn)) {
                        logger.log(Level.INFO, "Changes for {0}", columnName);
                        logger.log(Level.INFO, "Entity\t {0}", entityColumn);
                        logger.log(Level.INFO, "DB\t {0}", dbColumn);

                        //remove unique key
                        if (dbColumn.isUnique() && !entityColumn.isUnique()) {

                            logger.log(Level.INFO, "Remove unique key for {0}", columnName);

                            try (Statement statement = getStatement()) {
                                if (statement != null) {
                                    try (ResultSet rs = statement.executeQuery("SHOW INDEX FROM `" + tableName + "` WHERE Column_name = '" + dbColumn.getName() + "'")) {
                                        while (rs.next()) {
                                            String name = rs.getString("Key_name");
                                            try (Statement dStatement = getStatement()) {
                                                if (dStatement != null) {
                                                    dStatement.execute("ALTER TABLE `" + tableName + "` DROP INDEX `" + name + "`");
                                                } else {
                                                    logger.info(NULL_STATEMENT_MSG);
                                                }
                                            }
                                        }
                                    }

                                } else {
                                    logger.info(NULL_STATEMENT_MSG);
                                }
                            }
                        } //add unique key
                        else if (!dbColumn.isUnique() && entityColumn.isUnique()) {
                            logger.log(Level.INFO, "Add unique key for {0}", columnName);
                            try (Statement dStatement = getStatement()) {
                                if (dStatement != null) {
                                    dStatement.execute("ALTER TABLE `" + tableName + "` ADD UNIQUE (" + entityColumn.getName() + ")");
                                } else {
                                    logger.info(NULL_STATEMENT_MSG);
                                }
                            }
                        }

                        //remove nullable
                        if (!dbColumn.isNullable() && entityColumn.isNullable()) {

                            try (Statement dStatement = getStatement()) {
                                if (dStatement != null) {
                                    String q = "ALTER TABLE `" + tableName + "` MODIFY COLUMN " + entityColumn.getName() + " " + entityColumn.getType() + "(" + entityColumn.getSize() + ")";
                                    logger.log(Level.INFO, "Remove nullable for {0} query={1}", new Object[]{columnName, q});
                                    dStatement.execute(q);
                                } else {
                                    logger.info(NULL_STATEMENT_MSG);
                                }
                            }
                        } //add nullable
                        else if (dbColumn.isNullable() && !entityColumn.isNullable()) {

                            try (Statement dStatement = getStatement()) {
                                if (dStatement != null) {
                                    String q = "ALTER TABLE `" + tableName + "` MODIFY COLUMN " + entityColumn.getName() + " " + entityColumn.getType() + "(" + entityColumn.getSize() + ") null";
                                    logger.log(Level.INFO, "Add nullable for {0} query={1}", new Object[]{columnName, q});
                                    dStatement.execute(q);
                                } else {
                                    logger.info(NULL_STATEMENT_MSG);
                                }
                            }
                        }

                        //change size
                        if (dbColumn.getSize() != entityColumn.getSize()) {

                            try (Statement dStatement = getStatement()) {
                                if (dStatement != null) {
                                    StringBuilder builder = new StringBuilder("ALTER TABLE `" + tableName + "` CHANGE COLUMN `" + entityColumn.getName() + "` `" + entityColumn.getName() + "` " + entityColumn.getType() + " (" + entityColumn.getSize() + ")");

                                    if (entityColumn.isNullable()) {
                                        builder.append(" null");
                                    }

                                    logger.log(Level.INFO, "Change size for {0} query={1}", new Object[]{columnName, builder.toString()});
                                    dStatement.execute(builder.toString());
                                } else {
                                    logger.info(NULL_STATEMENT_MSG);
                                }
                            }

                        }

                    }

                }

            }

        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

    }

    private boolean tableExists() {

        try {
            String query = "SHOW TABLES FROM " + dbConnection.getCatalog() + " LIKE '" + tableName + "';";
            try (Statement statement = getStatement()) {
                if (statement != null) {
                    try (ResultSet rs = statement.executeQuery(query)) {
                        if (rs != null && rs.next()) {
                            logger.log(Level.INFO, "Table {0} exists.", tableName);
                            return true;
                        }
                    }
                } else {
                    logger.info(NULL_STATEMENT_MSG);
                }
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        logger.log(Level.INFO, "Table {0} does not exists.", tableName);
        return false;
    }

    private String getAddColumnQuery(Field field) {
        StringBuilder fieldQueryBuilder = new StringBuilder();
        List<String> pks = new ArrayList<>();
        try {
            field.setAccessible(true);

            if (ReflectUtil.fieldIsOkForDatabase(field)) {

                // attribute order matters in create query, so load
                // annotations, then add to query in particular order
                boolean primaryKey = false;
                boolean autoIncrement = false;
                boolean notNull = false;
                boolean unique = false;
                int size = -1;
                for (Annotation anno : field.getAnnotations()) {
                    if (anno instanceof DbPrimaryKey) {
                        primaryKey = true;
                    } else if (anno instanceof DbAutoIncrement) {
                        autoIncrement = true;
                    } else if (anno instanceof DbNotNull) {
                        notNull = true;
                    } else if (anno instanceof DbUnique) {
                        unique = true;
                    }
                }

                if (field.isAnnotationPresent(DbSize.class)) {
                    size = ReflectUtil.getSize(field);
                }

                fieldQueryBuilder.append("ALTER TABLE ").append(tableName).append(" ADD COLUMN `").append(ReflectUtil.getColumnName(field)).append("` ");
                fieldQueryBuilder.append(getColumnType(field));

                if (size != -1) {
                    fieldQueryBuilder.append("(").append(size).append(")");
                }

                if (notNull) {
                    fieldQueryBuilder.append(" NOT NULL ");
                }
                if (primaryKey) {
                    pks.add(ReflectUtil.getColumnName(field));
                }
                if (autoIncrement) {
                    fieldQueryBuilder.append(" AUTO_INCREMENT ");
                }
                if (unique) {
                    fieldQueryBuilder.append(" UNIQUE ");
                }

                if (!pks.isEmpty()) {
                    fieldQueryBuilder.append(", DROP PRIMARY KEY, ADD PRIMARY KEY(").append(getCommaList(pks)).append(")");
                }

            }

        } catch (SecurityException e) {
            logger.log(Level.SEVERE, null, e);
        }
        return fieldQueryBuilder.toString();
    }

    private String getCreateQuery() {

        StringBuilder queryBuilder = new StringBuilder("CREATE TABLE " + tableName + "(");

        T obj = getNewInstanceOfEntity();
        if (obj != null) {
            Class clazz = obj.getClass();
            if (clazz != null) {
                List<Field> fields = ReflectUtil.getAllFields(clazz);

                Collections.sort(fields, new Comparator<Field>() {
                    @Override
                    public int compare(Field f1, Field f2) {
                        return f1.getName().compareTo(f2.getName());
                    }
                });

                for (Field field : fields) {
                    logger.log(Level.FINE, "Field for create: {0}", field.getName());
                }

                List<String> fieldQueries = new ArrayList<>();
                List<String> pks = new ArrayList<>();

                for (Field field : fields) {

                    try {
                        field.setAccessible(true);

                        if (ReflectUtil.fieldIsOkForDatabase(field)) {
                            StringBuilder fieldQueryBuilder = new StringBuilder();

                            // attribute order matters in create query, so load
                            // annotations, then add to query in particular order
                            boolean primaryKey = false;
                            boolean autoIncrement = false;
                            boolean notNull = false;
                            boolean unique = false;
                            int size = -1;
                            for (Annotation anno : field.getAnnotations()) {
                                if (anno instanceof DbPrimaryKey) {
                                    primaryKey = true;
                                } else if (anno instanceof DbAutoIncrement) {
                                    autoIncrement = true;
                                } else if (anno instanceof DbNotNull) {
                                    notNull = true;
                                } else if (anno instanceof DbUnique) {
                                    unique = true;
                                }
                            }

                            if (field.isAnnotationPresent(DbSize.class)) {
                                size = ReflectUtil.getSize(field);
                            }

                            fieldQueryBuilder.append(ReflectUtil.getColumnName(field)).append(" ");
                            String dbType = getColumnType(field);
                            fieldQueryBuilder.append(dbType);

                            if (size != -1) {
                                fieldQueryBuilder.append("(" + size + ")");
                            } else if (dbType.equals(MySQLTypeConverter.DB_TYPE_INTEGER)) {
                                fieldQueryBuilder.append("(" + MySQLTypeConverter.DB_DEFAULT_INT_SIZE + ")");
                            }

                            if (notNull) {
                                fieldQueryBuilder.append(" NOT NULL ");
                            }
                            if (primaryKey) {
                                pks.add(ReflectUtil.getColumnName(field));
                            }
                            if (autoIncrement) {
                                fieldQueryBuilder.append(" AUTO_INCREMENT ");
                            }
                            if (unique) {
                                fieldQueryBuilder.append(" UNIQUE ");
                            }

                            fieldQueries.add(fieldQueryBuilder.toString());

                        }

                    } catch (SecurityException e) {
                        logger.log(Level.SEVERE, null, e);
                    }

                }

                int totalFieldCount = fieldQueries.size();
                int fieldCount = 1;

                for (String fieldQuery : fieldQueries) {
                    queryBuilder.append(fieldQuery);
                    if (fieldCount != totalFieldCount) {
                        queryBuilder.append(", ");
                    }
                    fieldCount++;
                }

                if (!pks.isEmpty()) {
                    queryBuilder.append(", PRIMARY KEY (").append(getCommaList(pks)).append(")");
                }

                queryBuilder.append(");");
            }
        }

        return queryBuilder.toString();
    }

    public void close() {
    }

    protected final Statement getStatement() {
        try {
            return dbConnection.createStatement();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
            return null;
        }
    }

    private String getCommaList(List<String> list) {
        StringBuilder builder = new StringBuilder();
        int itemCount = 1;
        for (String item : list) {
            builder.append(item);
            if (itemCount != list.size()) {
                builder.append(", ");
            }
            itemCount++;
        }
        return builder.toString();
    }

    /**
     * Convenience method that updates or inserts based on persisted flag
     *
     * @param entity
     * @return true if successful
     */
    public boolean save(T entity) {

        if (entity != null) {
            if (entity.isPersisted()) {
                update(entity);
                return true;
            } else {
                insert(entity);
                return true;
            }
        }
        logger.severe("Trying to save null entity");
        return false;

    }

    private String getEscapedField(Object value) {
        if (value != null) {
            if (value instanceof String) {
                return "\"" + value + "\"";
            } else if (value instanceof Boolean || value instanceof Integer || value instanceof Long || value instanceof Double || value instanceof Float) {
                return value + "";
            } else {
                return "\"" + value + "\"";
            }
        }
        logger.severe("Trying to escape null entity");
        return null;
    }

    public boolean insert(T entity) {

        HashMap<String, Object> cv = getData(entity);
        if (cv.size() > 0) {
            try {
                StringBuilder insertQueryBuilder = new StringBuilder("INSERT INTO " + tableName + "(");

                List<String> values = new ArrayList<>();
                List<String> columns = new ArrayList<>();

                for (Map.Entry<String, Object> entry : cv.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    columns.add(key);
                    values.add(getEscapedField(value));
                }

                insertQueryBuilder.append(getCommaList(columns)).append(") VALUES (");
                insertQueryBuilder.append(getCommaList(values)).append(");");
                logger.log(Level.FINE, "Insert query: {0}", insertQueryBuilder);

                String[] generatedColumns = {"id"};
                try (PreparedStatement statement = dbConnection.prepareStatement(insertQueryBuilder.toString(), generatedColumns)) {
                    if (statement != null) {
                        int affectedRows = statement.executeUpdate();

                        if (affectedRows > 0) {

                            try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
                                if (generatedKeys.next()) {
                                    entity.setId(generatedKeys.getLong(1));
                                } else {
                                    throw new SQLException("Creating user failed, no ID obtained.");
                                }
                            }

                        }
                    } else {
                        logger.severe(NULL_STATEMENT_MSG);
                    }
                }

            } catch (SQLException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }

        return false;
    }

    public boolean update(T entity) {

        HashMap<String, Object> cv = getData(entity);
        if (cv.size() > 0) {
            try {
                StringBuilder query = new StringBuilder("UPDATE " + tableName + " ");

                List<String> updates = new ArrayList<>();

                for (Map.Entry<String, Object> entry : cv.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    updates.add(" " + key + " = " + getEscapedField(value) + " ");
                }

                query.append("SET ").append(getCommaList(updates)).append(" WHERE id = ").append(entity.getId());

                logger.log(Level.FINE, "Update query: {0}", query);

                try (Statement statement = getStatement()) {
                    if (statement != null) {
                        statement.execute(query.toString());

                        return true;
                    } else {
                        logger.severe(NULL_STATEMENT_MSG);
                    }
                }

            } catch (SQLException ex) {
                logger.log(Level.SEVERE, null, ex);
                return false;
            }
        }
        return false;
    }

    public boolean delete(T entity) {

        HashMap cv = getData(entity);
        if (cv.size() > 0) {

            try {
                String query = "DELETE FROM " + tableName + " WHERE id = " + entity.getId();

                logger.log(Level.FINE, "Delete query: {0}", query);

                try (Statement statement = getStatement()) {
                    if (statement != null) {
                        statement.execute(query);

                    } else {
                        logger.severe(NULL_STATEMENT_MSG);
                    }
                }

                return true;
            } catch (SQLException ex) {
                logger.log(Level.SEVERE, "Delete failure", ex);
                return false;
            }

        }
        return false;
    }

    public T read(int id) {
        try {
            String query = "SELECT * FROM " + tableName + " WHERE id  = " + id;

            logger.log(Level.FINE, "Read query: {0}", query);

            try (Statement statement = getStatement()) {
                if (statement != null) {
                    try (ResultSet rs = statement.executeQuery(query)) {
                        if (rs.next()) {
                            T obj = setData(rs, getNewInstanceOfEntity());;
                            return obj;
                        }
                    }
                } else {
                    logger.severe(NULL_STATEMENT_MSG);
                }
            }
            return null;
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
            return null;
        }
    }

    /**
     * List records in table based on where X = ? clauses
     *
     * @param args where arguments
     * @return list of records for table
     */
    public List<T> listWhereArgsEquals(QueryArguments args) {

        List<T> list = new ArrayList<>();

        StringBuilder query = new StringBuilder("SELECT * FROM " + tableName);

        if (!args.getArgs().isEmpty()) {
            query.append(" WHERE ");

            int argCount = 1;
            for (Map.Entry<String, Object> entry : args.getArgs().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                query.append(key).append(" = ").append(getEscapedField(value)).append(" ");

                if (argCount != args.getArgs().size()) {
                    query.append(" AND ");
                }
                argCount++;
            }

        }
        logger.log(Level.FINE, "listWhereArgsEquals query: {0}", query);

        try {

            try (Statement statement = getStatement()) {
                if (statement != null) {
                    try (ResultSet rs = statement.executeQuery(query.toString())) {
                        while (rs != null && rs.next()) {
                            list.add(setData(rs, getNewInstanceOfEntity()));
                        }
                    }

                } else {
                    logger.severe(NULL_STATEMENT_MSG);
                }
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return list;
    }

    private T getNewInstanceOfEntity() {
        try {
            return entityClass.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            return null;
        }
    }

    /**
     * List all records for table
     *
     * @return list of records
     */
    public List<T> list() {
        QueryArguments args = new QueryArguments();
        return listWhereArgsEquals(args);
    }

    /**
     * List with custom query
     *
     * @param query
     * @return list of records
     */
    public List<T> list(String query) {
        List<T> list = new ArrayList<>();

        try {
            try (Statement statement = getStatement()) {
                if (statement != null) {
                    try (ResultSet rs = statement.executeQuery(query)) {
                        while (rs != null && rs.next()) {
                            list.add(setData(rs, getNewInstanceOfEntity()));
                        }
                    }

                } else {
                    logger.severe(NULL_STATEMENT_MSG);
                }
            }

        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return list;

    }

    public int count() {

        try {

            try (Statement statement = getStatement()) {
                if (statement != null) {
                    String cntField = "id";
                    if (ReflectUtil.noID(entityClass)) {
                        cntField = "*";
                    }

                    String query = "SELECT count(" + cntField + ") FROM " + tableName;

                    logger.log(Level.FINE, "Count query: {0}", query);

                    try (ResultSet resultSet = statement.executeQuery(query)) {
                        if (resultSet != null) {
                            while (resultSet.next()) {
                                return resultSet.getInt(1);
                            }
                        }
                    }
                } else {
                    logger.severe(NULL_STATEMENT_MSG);
                }
            }

        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    public int count(String whereClause) {

        try {

            try (Statement statement = getStatement()) {
                if (statement != null) {
                    String query = "SELECT count(id) FROM " + tableName + " WHERE " + whereClause;

                    logger.log(Level.FINE, "Count query: {0}", query);

                    try (ResultSet resultSet = statement.executeQuery(query)) {
                        if (resultSet != null) {
                            while (resultSet.next()) {
                                return resultSet.getInt(1);
                            }
                        }
                    }

                } else {
                    logger.severe(NULL_STATEMENT_MSG);
                }
            }

        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return 0;
    }

    private HashMap getData(T entity) {
        HashMap cv = new HashMap();

        if (entity != null) {
            List<Field> fields = ReflectUtil.getAllFields(entity.getClass());
            for (Field field : fields) {

                try {
                    field.setAccessible(true);

                    if (!ReflectUtil.containsIgnore(field)) {

                        Class<?> type = field.getType();
                        String columnName = ReflectUtil.getColumnName(field);

                        if (field.isAnnotationPresent(DbPrimaryKey.class) && ReflectUtil.noID(entity.getClass())) {
                            logger.finer("Entity has no ID annotation");
                        } else if (TYPE_CONVERTERS.containsKey(type)) {
                            Class<?> converterClazz = TYPE_CONVERTERS.get(type);
                            logger.log(Level.FINER, "{0} using type converter: {1}", new Object[]{field.getName(), converterClazz.getCanonicalName()});
                            AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                            Object obj = converter.getDatabaseValue(entity, field.getName());
                            logger.log(Level.FINER, "{0} value is {1}", new Object[]{field.getName(), converter.getDatabaseValue(entity, field.getName())});
                            putValue(cv, columnName, obj);
                        } else if (PRIMITIVE_TYPE_CONVERTERS.containsKey(type.getName())) {
                            Class<?> converterClazz = PRIMITIVE_TYPE_CONVERTERS.get(type.getName());
                            logger.log(Level.FINER, "{0} using primitive type converter: {1}", new Object[]{field.getName(), converterClazz.getCanonicalName()});
                            AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                            putValue(cv, columnName, converter.getDatabaseValue(entity, field.getName()));
                        } else {
                            logger.log(Level.FINER, "Unknown type {0} when getting data for field ''{1}''.", new Object[]{type.getName(), field.getName()});
                        }
                    }

                } catch (IllegalAccessException | IllegalArgumentException | ClassNotFoundException | InstantiationException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }

            }
        } else {
            logger.severe("Trying to get data from null entity");
        }

        return cv;
    }

    private T setData(ResultSet resultSet, T entityClassObject) {

        if (entityClassObject != null) {
            List<Field> fields = ReflectUtil.getAllFields(entityClassObject.getClass());
            for (Field field : fields) {

                try {
                    field.setAccessible(true);

                    String columnName = ReflectUtil.getColumnName(field);

                    // set entity as persisted when read
                    if (field.getName().equals("persisted")) {
                        field.set(entityClassObject, true);
                    }

                    if (field.isAnnotationPresent(DbPrimaryKey.class) && ReflectUtil.noID(entityClassObject.getClass())) {
                        logger.fine("Entity has no ID annotation");
                    } else if (!ReflectUtil.containsIgnore(field)) {
                        Class<?> type = field.getType();

                        if (TYPE_CONVERTERS.containsKey(type)) {
                            Class<?> converterClazz = TYPE_CONVERTERS.get(type);
                            logger.log(Level.FINER, "{0} using type converter: {1}", new Object[]{field.getName(), converterClazz.getCanonicalName()});
                            AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                            field.set(entityClassObject, converter.getValue(resultSet, columnName));
                        } else if (PRIMITIVE_TYPE_CONVERTERS.containsKey(type.getName())) {
                            Class<?> converterClazz = PRIMITIVE_TYPE_CONVERTERS.get(type.getName());
                            logger.log(Level.FINER, "{0} using primitive type converter: {1}", new Object[]{field.getName(), converterClazz.getCanonicalName()});
                            AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                            field.set(entityClassObject, converter.getValue(resultSet, columnName));
                        } else {
                            logger.log(Level.SEVERE, "Unknown type {0} when setting data for field ''{1}''.", new Object[]{type.getName(), field.getName()});
                        }
                    }

                } catch (IllegalAccessException | IllegalArgumentException | SQLException | ClassNotFoundException | InstantiationException e) {
                    logger.log(Level.SEVERE, null, e);
                }
            }
            return entityClassObject;
        } else {
            logger.info("Trying to set data from null entity");
            return null;
        }

    }

    private void putValue(HashMap values, String key, Object value) {

        if (values != null) {
            if (value instanceof Double) {
                values.put(key, (Double) value);
            } else if (value instanceof Float) {
                values.put(key, (Float) value);
            } else if (value instanceof Long) {
                values.put(key, (Long) value);
            } else if (value instanceof Integer) {
                values.put(key, (Integer) value);
            } else if (value instanceof Boolean) {
                values.put(key, (Boolean) value);
            } else if (value instanceof String) {
                values.put(key, (String) value);
            }
        } else {
            logger.severe("Trying to put data into null values");
        }

    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnType(Field field) {
        try {
            Class<?> type = field.getType();

            if (TYPE_CONVERTERS.containsKey(type)) {
                Class<?> converterClazz = TYPE_CONVERTERS.get(type);
                AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                return converter.getDatabaseType(field);
            } else if (PRIMITIVE_TYPE_CONVERTERS.containsKey(type.getName())) {
                Class<?> converterClazz = PRIMITIVE_TYPE_CONVERTERS.get(type.getName());
                AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                return converter.getDatabaseType(field);
            }
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            logger.log(Level.SEVERE, null, e);
        }
        return null;
    }

    public void setLogFile(File file) {
        logFile = file;
        if (!logFile.exists()) {
            try {
                boolean ok = logFile.createNewFile();
                if (!ok) {
                    logger.log(Level.WARNING, "Failed to create log file {0}", file);
                }
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
        }
    }

    public synchronized Logger getLogger() {
        return logger;
    }

}
