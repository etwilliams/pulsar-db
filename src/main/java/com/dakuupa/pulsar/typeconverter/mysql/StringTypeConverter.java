package com.dakuupa.pulsar.typeconverter.mysql;

import com.dakuupa.pulsar.Entity;
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
        }

        return DB_TYPE_VARCHAR;
    }

    @Override
    public String getDatabaseValue(Entity entity, String fieldName) {
        String val = (String) getFieldValue(entity, fieldName);
        return val;
    }

    @Override
    public String getValue(ResultSet rs, String columnName) throws SQLException {
        return rs.getString(columnName);
    }

}
