package com.dakuupa.pulsar.typeconverter.mysql;

import com.dakuupa.pulsar.Entity;
import static com.dakuupa.pulsar.typeconverter.mysql.MySQLTypeConverter.DB_TYPE_INTEGER;
import java.lang.reflect.Field;
import java.sql.ResultSet;

import java.sql.SQLException;

/**
 *
 * @author etwilliams
 */
public class IntegerTypeConverter extends MySQLTypeConverter<Integer> {

    @Override
    public String getDatabaseType(Field field) {
        return DB_TYPE_INTEGER;
    }

    @Override
    public Integer getDatabaseValue(Entity entity, String fieldName) {
        Integer val = (Integer) getFieldValue(entity, fieldName);
        return val;
    }

    @Override
    public Integer getValue(ResultSet rs, String columnName) throws SQLException {
        return rs.getInt(columnName);
    }

}
