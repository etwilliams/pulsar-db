package com.dakuupa.pulsar.typeconverter.mysql;

import com.dakuupa.pulsar.Entity;
import com.dakuupa.pulsar.annotations.DbMysqlLongText;
import com.dakuupa.pulsar.annotations.DbMysqlMediumText;
import com.dakuupa.pulsar.annotations.DbMysqlText;
import java.lang.reflect.Field;
import java.sql.ResultSet;

import java.sql.SQLException;

/**
 *
 * @author etwilliams
 */
public class StringTypeConverter extends MySQLTypeConverter<String> {

    @Override
    public String getDatabaseType(Field field) {

        if (field.isAnnotationPresent(DbMysqlText.class)) {
            return DB_TYPE_TEXT;
        } else if (field.isAnnotationPresent(DbMysqlMediumText.class)) {
            return DB_TYPE_MEDIUM_TEXT;
        } else if (field.isAnnotationPresent(DbMysqlLongText.class)) {
            return DB_TYPE_LONG_TEXT;
        }

        return DB_TYPE_VARCHAR;
    }

    @Override
    public String getDatabaseValue(Entity entity, String fieldName) {
        return (String) getFieldValue(entity, fieldName);
    }

    @Override
    public String getValue(ResultSet rs, String columnName) throws SQLException {
        return rs.getString(columnName);
    }

}
