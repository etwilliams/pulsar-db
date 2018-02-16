package com.dakuupa.pulsar.typeconverter;

import com.dakuupa.pulsar.Entity;
import java.lang.reflect.Field;
import java.sql.ResultSet;

import java.sql.SQLException;

/**
 *
 * @author etwilliams
 * @param <T> object type
 */
public interface TypeConverter<T> {
    
    String getDatabaseType(Field field);

    Object getDatabaseValue(Entity entity, String fieldName);

    T getValue(ResultSet rs, String columnName) throws SQLException;

    Object getFieldValue(Entity entity, String fieldName);

}
