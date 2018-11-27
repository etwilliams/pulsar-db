package com.dakuupa.pulsar.typeconverter.mysql;

import com.dakuupa.pulsar.Entity;
import java.lang.reflect.Field;
import java.sql.ResultSet;

import java.sql.SQLException;

/**
 *
 * @author etwilliams
 */
public class FloatTypeConverter extends MySQLTypeConverter<Float> {

    @Override
    public String getDatabaseType(Field field) {
        return DB_TYPE_FLOAT;
    }

    @Override
    public Float getDatabaseValue(Entity entity, String fieldName) {
        return (Float) getFieldValue(entity, fieldName);
    }

    @Override
    public Float getValue(ResultSet rs, String columnName) throws SQLException {
        return rs.getFloat(columnName);
    }

}
