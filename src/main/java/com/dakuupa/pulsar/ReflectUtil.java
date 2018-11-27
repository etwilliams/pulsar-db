package com.dakuupa.pulsar;

import com.dakuupa.pulsar.annotations.DbColumn;
import com.dakuupa.pulsar.annotations.DbIgnore;
import com.dakuupa.pulsar.annotations.DbSize;
import com.dakuupa.pulsar.annotations.DbTable;
import com.dakuupa.pulsar.annotations.NoID;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class to ease reflection
 *
 * @author EWilliams
 *
 */
public class ReflectUtil {

    private ReflectUtil(){
        //hide implicit public constructor
    }
    
    public static List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
        if (clazz.getSuperclass() != null) {
            fields.addAll(getAllFields(clazz.getSuperclass()));
        }
        return fields;
    }

    /**
     *
     * @param field
     * @return true if field has Ignore annotation
     */
    public static boolean containsIgnore(Field field) {
        for (Annotation anno : field.getAnnotations()) {
            if (anno instanceof DbIgnore) {
                return true;
            }
        }
        return false;
    }

    /**
     *
     * @param clazz
     * @return true if field has Ignore annotation
     */
    public static String tableName(Class clazz) {
        if (clazz.isAnnotationPresent(DbTable.class)) {
            String tableName = ((DbTable) (clazz.getAnnotation(DbTable.class))).name();
            return tableName.toLowerCase();
        }
        return null;
    }

    /**
     * @param field
     * @return columnName annotation value
     */
    public static String getColumnName(Field field) {
        if (field.isAnnotationPresent(DbColumn.class)) {
            return (field.getAnnotation(DbColumn.class)).name();
        } else {
            return field.getName();
        }
    }
    
    /**
     * @param field
     * @return columnName annotation value
     */
    public static int getSize(Field field) {
        if (field.isAnnotationPresent(DbSize.class)) {
            return (field.getAnnotation(DbSize.class)).size();
        } else {
            return 1024;
        }
    }

    /**
     *
     * @param clazz
     * @return true if class has NoID annotation
     */
    public static boolean noID(Class clazz) {
        return clazz.isAnnotationPresent(NoID.class);
    }

    public static boolean fieldIsOkForDatabase(Field field) {
        return !(containsIgnore(field)
                || field.getName().equals("serialVersionUID")
                || field.getName().equals("INVALID_ID")
                || field.getName().equals("persisted")
                || field.getName().contains("$"));
    }
}
