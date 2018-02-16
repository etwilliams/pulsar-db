package com.dakuupa.pulsar.typeconverter;

import com.dakuupa.pulsar.Entity;
import com.dakuupa.pulsar.ReflectUtil;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author etwilliams
 * @param <T> object type
 */
public abstract class AbstractTypeConverter<T> implements TypeConverter<T> {
    
    @Override
    public abstract String getDatabaseType(Field field);

    @Override
    public abstract Object getDatabaseValue(Entity entity, String fieldName);

    @Override
    public abstract T getValue(ResultSet rs, String columnName) throws SQLException;

    @Override
    public Object getFieldValue(Entity entity, String fieldName) {
        try {
            Class<?> clazz = entity.getClass();

            for (Field cField : ReflectUtil.getAllFields(clazz)) {
                cField.setAccessible(true);
                if (cField.getName().equals(fieldName)) {
                    Object objVal = cField.get(entity);
                    return objVal;
                }
            }

        } catch (SecurityException | IllegalArgumentException | IllegalAccessException ex) {
            Logger.getLogger(AbstractTypeConverter.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

}
