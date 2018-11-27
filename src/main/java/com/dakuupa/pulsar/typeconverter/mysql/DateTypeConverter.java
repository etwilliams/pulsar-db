package com.dakuupa.pulsar.typeconverter.mysql;

import com.dakuupa.pulsar.Entity;
import java.lang.reflect.Field;
import java.sql.ResultSet;

import java.sql.SQLException;
import java.util.Date;

/**
 *
 * @author etwilliams
 */
public class DateTypeConverter extends MySQLTypeConverter<Date> {

    @Override
    public String getDatabaseType(Field field) {
        return DB_TYPE_LONG;
    }

    @Override
    public Long getDatabaseValue(Entity entity, String fieldName) {
        Date val = (Date) getFieldValue(entity, fieldName);
        if (val != null) {
            return val.getTime();
        }
        return 0L;
    }

    @Override
    public Date getValue(ResultSet rs, String columnName) throws SQLException {
        long val = rs.getLong(columnName);
        return new Date(val);
    }

}
