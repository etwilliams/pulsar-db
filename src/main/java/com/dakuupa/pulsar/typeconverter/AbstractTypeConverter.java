package com.dakuupa.pulsar.typeconverter;

import com.dakuupa.pulsar.Entity;
import com.dakuupa.pulsar.ReflectUtil;
import java.lang.reflect.Field;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author etwilliams
 * @param <T> object type
 */
public abstract class AbstractTypeConverter<T> implements TypeConverter<T> {

    @Override
    public Object getFieldValue(Entity entity, String fieldName) {
        try {
            Class<?> clazz = entity.getClass();

            for (Field cField : ReflectUtil.getAllFields(clazz)) {
                cField.setAccessible(true);
                if (cField.getName().equals(fieldName)) {
                    return cField.get(entity);
                }
            }

        } catch (SecurityException | IllegalArgumentException | IllegalAccessException ex) {
            Logger.getLogger(AbstractTypeConverter.class.getName()).log(Level.SEVERE, null, ex);
        }

        return null;
    }

}
