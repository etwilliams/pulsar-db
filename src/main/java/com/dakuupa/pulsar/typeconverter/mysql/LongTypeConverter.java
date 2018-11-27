package com.dakuupa.pulsar.typeconverter.mysql;

import com.dakuupa.pulsar.Entity;
import java.lang.reflect.Field;
import java.sql.ResultSet;

import java.sql.SQLException;

/**
 *
 * @author etwilliams
 */
public class LongTypeConverter extends MySQLTypeConverter<Long> {

    @Override
    public String getDatabaseType(Field field) {
        return DB_TYPE_LONG;
    }

    @Override
    public Long getDatabaseValue(Entity entity, String fieldName) {
        return (Long) getFieldValue(entity, fieldName);
    }

    @Override
    public Long getValue(ResultSet rs, String columnName) throws SQLException {
        return rs.getLong(columnName);
    }

}
