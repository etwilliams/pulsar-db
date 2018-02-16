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
        Long val = (Long) getFieldValue(entity, fieldName);
        return val;
    }

    @Override
    public Long getValue(ResultSet rs, String columnName) throws SQLException {
        return rs.getLong(columnName);
    }

}
