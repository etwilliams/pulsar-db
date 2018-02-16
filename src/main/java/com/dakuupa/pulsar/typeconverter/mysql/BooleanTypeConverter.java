package com.dakuupa.pulsar.typeconverter.mysql;

import com.dakuupa.pulsar.Entity;
import java.lang.reflect.Field;
import java.sql.ResultSet;

import java.sql.SQLException;

/**
 *
 * @author etwilliams
 */
public class BooleanTypeConverter extends MySQLTypeConverter<Boolean> {

    @Override
    public String getDatabaseType(Field field) {
        return DB_TYPE_BOOLEAN;
    }

    @Override
    public Boolean getDatabaseValue(Entity entity, String fieldName) {
        Boolean val = (Boolean) getFieldValue(entity, fieldName);
        return val;
    }

    @Override
    public Boolean getValue(ResultSet rs, String columnName) throws SQLException {
        return rs.getBoolean(columnName);
    }

}
