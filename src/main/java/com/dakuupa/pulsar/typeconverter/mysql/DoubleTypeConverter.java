package com.dakuupa.pulsar.typeconverter.mysql;

import com.dakuupa.pulsar.Entity;
import java.lang.reflect.Field;
import java.sql.ResultSet;

import java.sql.SQLException;

/**
 *
 * @author etwilliams
 */
public class DoubleTypeConverter extends MySQLTypeConverter<Double> {

    @Override
    public String getDatabaseType(Field field) {
        return DB_TYPE_DOUBLE;
    }

    @Override
    public Double getDatabaseValue(Entity entity, String fieldName) {
        Double val = (Double) getFieldValue(entity, fieldName);
        return val;
    }

    @Override
    public Double getValue(ResultSet rs, String columnName) throws SQLException {
        return rs.getDouble(columnName);
    }

}
