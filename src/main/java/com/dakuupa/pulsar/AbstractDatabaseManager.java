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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base DB Manager Allow types are double, float, long, integer, boolean, and String values.
 *
 * @author EWilliams
 *
 * @param <T> DB class that extends Entity
 */
public abstract class AbstractDatabaseManager<T extends Entity> {

    private String tableName;
    private Connection dbConnection;
    private Class<T> entityClass;
    private boolean debug = true;

    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
    private static File logFile;
    private static boolean enableLogging;
    private static boolean logToStdOut;

    private static final HashMap<Class, Class> typeConverters = new HashMap<>();
    private static final HashMap<String, Class> primitiveTypeConverters = new HashMap<>();

    static {
        //initialize default type converters
        typeConverters.put(String.class, StringTypeConverter.class);
        typeConverters.put(Integer.class, IntegerTypeConverter.class);
        typeConverters.put(Double.class, DoubleTypeConverter.class);
        typeConverters.put(Long.class, LongTypeConverter.class);
        typeConverters.put(Float.class, FloatTypeConverter.class);
        typeConverters.put(Boolean.class, BooleanTypeConverter.class);
        typeConverters.put(Date.class, DateTypeConverter.class);

        //init primitive types also
        primitiveTypeConverters.put("int", IntegerTypeConverter.class);
        primitiveTypeConverters.put("double", DoubleTypeConverter.class);
        primitiveTypeConverters.put("long", LongTypeConverter.class);
        primitiveTypeConverters.put("float", FloatTypeConverter.class);
        primitiveTypeConverters.put("boolean", BooleanTypeConverter.class);
    }

    public AbstractDatabaseManager(Connection con) {
        init(con, (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
    }
    
    public AbstractDatabaseManager(Connection con, TypeConverter... converters) {
        enableLogging = true;
        logToStdOut = true;
        
        for (TypeConverter converter : converters){
            Class<T> clazz = (Class<T>)((ParameterizedType)converter.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
            typeConverters.put(clazz, converter.getClass());
        }
        
        init(con, (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
    }

    public AbstractDatabaseManager(Connection con, Class<T> entityClass) {
        init(con, entityClass);
    }

    private void init(Connection con, Class<T> entityClass) {
        dbConnection = con;

        this.entityClass = entityClass;
        this.tableName = entityClass.getSimpleName().toLowerCase();

        String tableNameAnno = ReflectUtil.tableName(entityClass);
        if (tableNameAnno != null) {
            this.tableName = tableNameAnno.toLowerCase();
        }

        for (Class key : typeConverters.keySet()) {
            log("Type Converter " + key.getCanonicalName());
        }
        for (String key : primitiveTypeConverters.keySet()) {
            log("Primitive Type Converter " + key);
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
                log("Creating table " + tableName + " using query: " + query);

                Statement statement = getStatement();
                statement.execute(query);
                statement.close();
                statement = null;

            } else {

                //compare for changes
                //load DB column info
                Statement statement = getStatement();
                ResultSet rs = statement.executeQuery("SHOW COLUMNS FROM " + tableName);
                while (rs.next()) {
                    Column col = new Column(rs);
                    dbColumns.put(col.getName(), col);
                    dbColumnNames.add(col.getName());
                }
                rs.close();
                rs = null;
                statement.close();
                statement = null;

                //load Entity column info
                List<Field> fields = ReflectUtil.getAllFields(getNewInstanceOfEntity().getClass());
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

                //add new columns
                for (Field added : addedColumns) {
                    String addQuery = getAddColumnQuery(added);
                    if (addQuery != null && !addQuery.isEmpty()) {
                        log("Add column query: " + addQuery);

                        Column col = new Column(added, this);
                        dbColumns.put(col.getName(), col);

                        statement = getStatement();
                        statement.execute(addQuery);
                        statement.close();

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
                        log("Remove column query: " + dropQuery);
                        statement = getStatement();
                        statement.execute(dropQuery);
                        statement.close();
                        dbColumns.remove(col);
                    }
                }

                //modify existing columns
                for (Map.Entry<String, Column> entry : entityColumns.entrySet()) {
                    String columnName = entry.getKey();
                    Column entityColumn = entry.getValue();
                    Column dbColumn = dbColumns.get(columnName);

                    if (!entityColumn.equals(dbColumn)) {
                        log("Column changes for " + columnName);
                        log("Entity\t " + entityColumn.toString());
                        log("DB\t " + dbColumn.toString());

                        //remove unique key
                        if (dbColumn.isUnique() && !entityColumn.isUnique()) {

                            log("Remove unique key for " + columnName);

                            statement = getStatement();
                            rs = statement.executeQuery("SHOW INDEX FROM `" + tableName + "` WHERE Column_name = '" + dbColumn.getName() + "'");
                            while (rs.next()) {
                                String name = rs.getString("Key_name");
                                Statement dStatement = getStatement();
                                dStatement.execute("ALTER TABLE `" + tableName + "` DROP INDEX `" + name + "`");
                                dStatement.close();
                                dStatement = null;
                            }
                            rs.close();
                            rs = null;
                            statement.close();
                            statement = null;
                        } //add unique key
                        else if (!dbColumn.isUnique() && entityColumn.isUnique()) {
                            log("Add unique key for " + columnName);
                            Statement dStatement = getStatement();
                            dStatement.execute("ALTER TABLE `" + tableName + "` ADD UNIQUE (" + entityColumn.getName() + ")");
                            dStatement.close();
                            dStatement = null;
                        }

                        //remove nullable
                        if (!dbColumn.isNullable() && entityColumn.isNullable()) {

                            Statement dStatement = getStatement();
                            String q = "ALTER TABLE `" + tableName + "` MODIFY COLUMN " + entityColumn.getName() + " " + entityColumn.getType() + "(" + entityColumn.getSize() + ")";
                            log("Remove nullable for " + columnName + " query=" + q);
                            dStatement.execute(q);
                            dStatement.close();
                            dStatement = null;
                        } //add nullable
                        else if (dbColumn.isNullable() && !entityColumn.isNullable()) {

                            Statement dStatement = getStatement();
                            String q = "ALTER TABLE `" + tableName + "` MODIFY COLUMN " + entityColumn.getName() + " " + entityColumn.getType() + "(" + entityColumn.getSize() + ") null";
                            log("Add nullable for " + columnName + " query=" + q);
                            dStatement.execute(q);
                            dStatement.close();
                            dStatement = null;
                        }

                        //change size
                        if (dbColumn.getSize() != entityColumn.getSize()) {

                            Statement dStatement = getStatement();
                            String q = "ALTER TABLE `" + tableName + "` CHANGE COLUMN `" + entityColumn.getName() + "` `" + entityColumn.getName() + "` " + entityColumn.getType() + " (" + entityColumn.getSize() + ")";

                            if (entityColumn.isNullable()) {
                                q += " null";
                            }

                            log("Change size for " + columnName + " query=" + q);
                            dStatement.execute(q);
                            dStatement.close();
                            dStatement = null;

                        }

                    }

                }

            }

        } catch (SQLException ex) {
            Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private boolean tableExists() {

        try {
            String query = "SHOW TABLES FROM " + dbConnection.getCatalog() + " LIKE '" + tableName + "';";
            Statement statement = getStatement();
            ResultSet rs = statement.executeQuery(query);

            if (rs.next()) {
                rs.close();
                rs = null;
                statement.close();
                statement = null;
                log("Table " + tableName + " exists.");
                return true;
            }

        } catch (SQLException ex) {
            Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        log("Table " + tableName + " does not exists.");
        return false;
    }

    private String getAddColumnQuery(Field field) {
        String fieldQuery = "";
        List<String> pks = new ArrayList<>();
        try {
            field.setAccessible(true);

            if (ReflectUtil.fieldIsOkForDatabase(field)) {

                Class<?> type = field.getType();

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

                fieldQuery += "ALTER TABLE " + tableName + " ADD COLUMN `" + ReflectUtil.getColumnName(field) + "` ";

                fieldQuery += getColumnType(field);

                if (size != -1) {
                    fieldQuery += "(" + size + ")";
                }

                if (notNull) {
                    fieldQuery += " NOT NULL ";
                }
                if (primaryKey) {
                    pks.add(ReflectUtil.getColumnName(field));
                }
                if (autoIncrement) {
                    fieldQuery += " AUTO_INCREMENT ";
                }
                if (unique) {
                    fieldQuery += " UNIQUE ";
                }

                if (!pks.isEmpty()) {
                    fieldQuery += ", DROP PRIMARY KEY, ADD PRIMARY KEY(" + getCommaList(pks) + ")";
                }

            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return fieldQuery;
    }

    private String getCreateQuery() {

        String query = "CREATE TABLE " + tableName + "(";

        List<Field> fields = ReflectUtil.getAllFields(getNewInstanceOfEntity().getClass());

        Collections.sort(fields, new Comparator<Field>() {
            @Override
            public int compare(Field f1, Field f2) {
                return f1.getName().compareTo(f2.getName());
            }
        });

        for (Field field : fields) {
            log("Field for create: " + field.getName());
        }

        List<String> fieldQueries = new ArrayList<>();
        List<String> pks = new ArrayList<>();

        for (Field field : fields) {

            try {
                field.setAccessible(true);

                if (ReflectUtil.fieldIsOkForDatabase(field)) {
                    String fieldQuery = "";
                    Class<?> type = field.getType();

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

                    fieldQuery += ReflectUtil.getColumnName(field) + " ";
                    String dbType = "";

                    dbType = getColumnType(field);
                    fieldQuery += dbType;

                    if (size != -1) {
                        fieldQuery += "(" + size + ")";
                    } else if (dbType.equals(MySQLTypeConverter.DB_TYPE_INTEGER)) {
                        fieldQuery += "(" + MySQLTypeConverter.DB_DEFAULT_INT_SIZE + ")";
                    }

                    if (notNull) {
                        fieldQuery += " NOT NULL ";
                    }
                    if (primaryKey) {
                        pks.add(ReflectUtil.getColumnName(field));
                    }
                    if (autoIncrement) {
                        fieldQuery += " AUTO_INCREMENT ";
                    }
                    if (unique) {
                        fieldQuery += " UNIQUE ";
                    }

                    fieldQueries.add(fieldQuery);

                }

            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

        int totalFieldCount = fieldQueries.size();
        int fieldCount = 1;

        for (String fieldQuery : fieldQueries) {
            query += fieldQuery;
            if (fieldCount != totalFieldCount) {
                query += ", ";
            }
            fieldCount++;
        }

        if (!pks.isEmpty()) {
            query += ", PRIMARY KEY (" + getCommaList(pks) + ")";
        }

        query += ");";

        return query;
    }

    public void close() {
    }

    protected final Statement getStatement() {
        try {
            return (Statement) dbConnection.createStatement();
        } catch (SQLException ex) {
            Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    private String getCommaList(List<String> list) {
        String str = "";
        int itemCount = 1;
        for (String item : list) {
            str += item;
            if (itemCount != list.size()) {
                str += ", ";
            }
            itemCount++;
        }
        return str;
    }

    /**
     * Convenience method that updates or inserts based on persisted flag
     *
     * @param entity
     * @return true if successful
     */
    public boolean save(T entity) {

        if (entity.isPersisted()) {//|| entity.getId() != Entity.INVALID_ID) {
            update(entity);
            return true;
        } else {
            insert(entity);
            return true;
        }

    }

    private String getEscapedField(Object value) {
        if (value instanceof String) {
            return "\"" + value + "\"";
        } else if (value instanceof Boolean || value instanceof Integer || value instanceof Long || value instanceof Double || value instanceof Float) {
            return value + "";
        } else {
            return "\"" + value + "\"";
        }
    }

    public boolean insert(T entity) {

        ResultSet rs = null;
        Statement statement = null;

        HashMap<String, Object> cv = getData(entity);
        if (cv.size() > 0) {
            try {
                String insertQuery = "INSERT INTO " + tableName + "(";

                List<String> values = new ArrayList<>();
                List<String> columns = new ArrayList<>();

                for (Map.Entry<String, Object> entry : cv.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    columns.add(key);
                    values.add(getEscapedField(value));
                }

                insertQuery += getCommaList(columns) + ") VALUES (";
                insertQuery += getCommaList(values) + ");";
                if (debug) {
                    log("Insert query: " + insertQuery);
                }

                statement = getStatement();
                statement.execute(insertQuery);
                statement.close();

                //grab last id
                //TODO this is sketchy
                statement = getStatement();
                rs = statement.executeQuery("select last_insert_id() as last_id from " + tableName);

                if (rs.next()) {
                    int rowId = Integer.parseInt(rs.getString("last_id"));

                    if (rowId != -1) {
                        entity.setId(rowId);
                        entity.setPersisted(true);
                        rs.close();
                        rs = null;

                        statement.close();
                        statement = null;

                        return true;
                    }
                }

            } catch (SQLException ex) {
                Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                        rs = null;
                    } catch (SQLException ex) {
                        Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                if (statement != null) {
                    try {
                        statement.close();
                        statement = null;
                    } catch (SQLException ex) {
                        Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }

        return false;
    }

    public boolean update(T entity) {

        Statement statement = null;
        HashMap<String, Object> cv = getData(entity);
        if (cv.size() > 0) {
            try {
                String query = "UPDATE " + tableName + " ";

                List<String> updates = new ArrayList<>();

                for (Map.Entry<String, Object> entry : cv.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    updates.add(" " + key + " = " + getEscapedField(value) + " ");
                }

                query += "SET " + getCommaList(updates) + " WHERE id = " + entity.getId();

                if (debug) {
                    log("Update query: " + query);
                }

                statement = getStatement();
                statement.execute(query);
                statement.close();
                statement = null;

                return true;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                        statement = null;
                    } catch (SQLException ex) {
                        Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }
        return false;
    }

    public boolean delete(T entity) {
        Statement statement = null;
        HashMap cv = getData(entity);
        if (cv.size() > 0) {

            try {
                String query = "DELETE FROM " + tableName + " WHERE id = " + entity.getId();

                if (debug) {
                    log("Delete query: " + query);
                }

                statement = getStatement();
                statement.execute(query);

                statement.close();
                statement = null;

                return true;
            } catch (SQLException ex) {
                ex.printStackTrace();
                return false;
            } finally {

                if (statement != null) {
                    try {
                        statement.close();
                        statement = null;
                    } catch (SQLException ex) {
                        Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }

        }
        return false;
    }

    public T read(int id) {
        ResultSet rs = null;
        Statement statement = null;
        try {
            String query = "SELECT * FROM " + tableName + " WHERE id  = " + id;

            if (debug) {
                log("Read query: " + query);
            }

            statement = getStatement();
            rs = statement.executeQuery(query);

            if (rs.next()) {
                T obj = (T) setData(rs, getNewInstanceOfEntity());;
                rs.close();
                rs = null;
                statement.close();
                statement = null;
                return obj;
            }
            return null;
        } catch (SQLException ex) {
            Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (SQLException ex) {
                    Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                }
                if (statement != null) {
                    try {
                        statement.close();
                        statement = null;
                    } catch (SQLException ex) {
                        Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
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

        String query = "SELECT * FROM " + tableName;

        if (!args.getArgs().isEmpty()) {
            query += " WHERE ";

            int argCount = 1;
            for (Map.Entry<String, Object> entry : args.getArgs().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                query += key + " = " + getEscapedField(value) + " ";

                if (argCount != args.getArgs().size()) {
                    query += " AND ";
                }
                argCount++;
            }

        }
        if (debug) {
            log("listWhereArgsEquals query: " + query);
        }

        Statement statement = getStatement();
        ResultSet rs = null;
        try {
            rs = statement.executeQuery(query);
            while (rs.next()) {
                //System.out.println("next RS:" + rs.toString());
                list.add(setData(rs, getNewInstanceOfEntity()));
            }
            rs.close();
            rs = null;
            statement.close();
            statement = null;
            return list;
        } catch (SQLException ex) {
            Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (SQLException ex) {
                    Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                    statement = null;
                } catch (SQLException ex) {
                    Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

    }

    private T getNewInstanceOfEntity() {
        try {
            return (T) entityClass.newInstance();
        } catch (Exception e) {
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
        ResultSet rs = null;
        Statement statement = getStatement();
        try {
            rs = statement.executeQuery(query);

            while (rs.next()) {
                list.add(setData(rs, getNewInstanceOfEntity()));
            }
            rs.close();
            rs = null;

            statement.close();
            statement = null;

            return list;
        } catch (SQLException ex) {
            Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                    rs = null;
                } catch (SQLException ex) {
                    Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                    statement = null;
                } catch (SQLException ex) {
                    Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public int count() {
        Statement statement = getStatement();
        ResultSet resultSet = null;
        try {

            String cntField = "id";
            if (ReflectUtil.noID(entityClass)) {
                cntField = "*";
            }

            String query = "SELECT count(" + cntField + ") FROM " + tableName;

            if (debug) {
                log("Count query: " + query);
            }

            resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                int count = resultSet.getInt(1);
                resultSet.close();
                statement.close();
                statement = null;
                return count;
            }

        } catch (SQLException ex) {
            Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException ex) {
                    Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                    statement = null;
                } catch (SQLException ex) {
                    Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return 0;
    }

    public int count(String whereClause) {
        ResultSet resultSet = null;
        Statement statement = getStatement();
        try {
            String query = "SELECT count(id) FROM " + tableName + " WHERE " + whereClause;

            if (debug) {
                log("Count query: " + query);
            }

            resultSet = getStatement().executeQuery(query);
            resultSet = statement.executeQuery(query);
            statement.close();
            statement = null;

            while (resultSet.next()) {
                int count = resultSet.getInt(1);
                resultSet.close();
                return count;
            }

        } catch (SQLException ex) {
            Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException ex) {
                    Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                    statement = null;
                } catch (SQLException ex) {
                    Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return 0;
    }

    private HashMap getData(T entity) {
        HashMap cv = new HashMap();

        List<Field> fields = ReflectUtil.getAllFields(entity.getClass());
        for (Field field : fields) {

            try {
                field.setAccessible(true);

                if (!ReflectUtil.containsIgnore(field)) {

                    Class<?> type = field.getType();
                    String columnName = ReflectUtil.getColumnName(field);

                    if (field.isAnnotationPresent(DbPrimaryKey.class) && ReflectUtil.noID(entity.getClass())) {
                        log("Entity has no ID annotation");
                    } else if (typeConverters.containsKey(type)) {
                        Class<?> converterClazz = typeConverters.get(type);
                        log(field.getName() + " using type converter: " + converterClazz.getCanonicalName());
                        AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                        Object obj = converter.getDatabaseValue(entity, field.getName());
                        log(field.getName() + " value is "+ converter.getDatabaseValue(entity, field.getName()));
                        putValue(cv, columnName, obj);
                    } else if (primitiveTypeConverters.containsKey(type.getName())) {
                        Class<?> converterClazz = primitiveTypeConverters.get(type.getName());
                        log(field.getName() + " using primitive type converter: " + converterClazz.getCanonicalName());
                        AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                        putValue(cv, columnName, converter.getDatabaseValue(entity, field.getName()));
                    } else {
                        log("Unknown type " + type.getName() + " when getting data for field '" + field.getName() + "'.");
                    }
                }

            } catch (IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InstantiationException ex) {
                Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        return cv;
    }

    private T setData(ResultSet resultSet, T entityClassObject) {

        //System.out.println("setData RS:" + resultSet.toString());
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
                    log("Entity has no ID annotation");
                } else if (!ReflectUtil.containsIgnore(field)) {
                    Class<?> type = field.getType();

                    if (typeConverters.containsKey(type)) {
                        Class<?> converterClazz = typeConverters.get(type);
                        log(field.getName() + " using type converter: " + converterClazz.getCanonicalName());
                        AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                        field.set(entityClassObject, converter.getValue(resultSet, columnName));
                    } else if (primitiveTypeConverters.containsKey(type.getName())) {
                        Class<?> converterClazz = primitiveTypeConverters.get(type.getName());
                        log(field.getName() + " using primitive type converter: " + converterClazz.getCanonicalName());
                        AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                        field.set(entityClassObject, converter.getValue(resultSet, columnName));
                    } else {
                        log("Unknown type " + type.getName() + " when setting data for field '" + field.getName() + "'.");
                    }
                }

            } catch (IllegalAccessException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return (T) entityClassObject;
    }

    private void putValue(HashMap values, String key, Object value) {

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

    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public static HashMap<String, Class> getPrimitiveTypeConverters() {
        return primitiveTypeConverters;
    }

    public static HashMap<Class, Class> getTypeConverters() {
        return typeConverters;
    }

    public String getColumnType(Field field) {
        try {
            Class<?> type = field.getType();

            if (typeConverters.containsKey(type)) {
                Class<?> converterClazz = typeConverters.get(type);
                AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                return converter.getDatabaseType(field);
            } else if (primitiveTypeConverters.containsKey(type.getName())) {
                Class<?> converterClazz = primitiveTypeConverters.get(type.getName());
                AbstractTypeConverter converter = (AbstractTypeConverter) Class.forName(converterClazz.getCanonicalName()).newInstance();
                return converter.getDatabaseType(field);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void setLogFile(File file) {
        logFile = file;
        if (!logFile.exists()) {
            try {
                logFile.createNewFile();
            } catch (IOException ex) {
                Logger.getLogger(AbstractDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void setLoggingEnabled(boolean enabled) {
        enableLogging = enabled;
    }

    public static void setLogToStdOut(boolean logToStdOut) {
        AbstractDatabaseManager.logToStdOut = logToStdOut;
    }

    private void log(String log) {
        if (enableLogging) {
            String msg = DATE_FORMATTER.format(new Date()) + " " + getClass().getSimpleName() + "\t" + log;
            if (logToStdOut){
                System.out.println(msg);
            }
            if (logFile != null && logFile.exists() && logFile.canWrite()) {
                try (FileOutputStream fos = new FileOutputStream(logFile, true); OutputStreamWriter osw = new OutputStreamWriter(fos)) {
                    osw.write(msg);
                    osw.write("\n");
                    osw.flush();
                    osw.close();
                } catch (Exception ex) {

                }
            }
        }
    }
}
