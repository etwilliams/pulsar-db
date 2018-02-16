package com.dakuupa.pulsar;

import com.dakuupa.pulsar.annotations.DbAutoIncrement;
import com.dakuupa.pulsar.annotations.DbNotNull;
import com.dakuupa.pulsar.annotations.DbPrimaryKey;
import com.dakuupa.pulsar.annotations.DbSize;
import com.dakuupa.pulsar.annotations.DbUnique;
import com.dakuupa.pulsar.typeconverter.mysql.IntegerTypeConverter;
import com.dakuupa.pulsar.typeconverter.mysql.MySQLTypeConverter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 *
 * @author etwilliams
 */
public class Column {

    private String name;
    private String type;
    private int size;
    private boolean nullable;
    private boolean primaryKey;
    private boolean autoIncrement;
    private boolean unique;

    public Column() {
    }

    public Column(Field field, AbstractDatabaseManager manager) {

        name = ReflectUtil.getColumnName(field);
        type = manager.getColumnType(field);
        nullable = true;

        for (Annotation anno : field.getAnnotations()) {
            if (anno instanceof DbPrimaryKey) {
                primaryKey = true;
            } else if (anno instanceof DbAutoIncrement) {
                autoIncrement = true;
            } else if (anno instanceof DbNotNull) {
                nullable = false;
            } else if (anno instanceof DbUnique) {
                unique = true;
            }
        }

        if (field.isAnnotationPresent(DbSize.class)) {
            size = ReflectUtil.getSize(field);
        } else if (type.equals(IntegerTypeConverter.DB_TYPE_INTEGER) && size == 0) {
            size = MySQLTypeConverter.DB_DEFAULT_INT_SIZE;
        } else if (type.equals(IntegerTypeConverter.DB_TYPE_BOOLEAN) && size == 0) {
            size = MySQLTypeConverter.DB_DEFAULT_BOOLEAN_SIZE;
        }
        /*else if ((type.equals(IntegerTypeConverter.DB_TYPE_BOOLEAN)
                || type.equals(IntegerTypeConverter.DB_TYPE_FLOAT)
                || type.equals(IntegerTypeConverter.DB_TYPE_LONG)
                || type.equals(IntegerTypeConverter.DB_TYPE_DOUBLE))
                && size == 0) {
            size = MySQLTypeConverter.DB_DEFAULT_NUMBER_SIZE;
        }*/
    }

    public Column(ResultSet rs) throws SQLException {
        String columnNameField = rs.getString("Field");
        String typeField = rs.getString("Type");
        String nullableField = rs.getString("Null");
        String keyField = rs.getString("Key");
        //String defaultValField = rs.getString("Default");
        String extraField = rs.getString("Extra");

        name = columnNameField;
        try {
            size = Integer.parseInt(typeField.split("[\\(\\)]")[1]);
        } catch (Exception e) {
            //ignore
        }

        type = typeField.replace("(" + size + ")", "").toUpperCase();

        nullable = nullableField.equals("YES");
        primaryKey = keyField.equals("PRI");
        unique = keyField.equals("UNI");
        autoIncrement = extraField.contains("auto_increment");

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        this.primaryKey = primaryKey;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Column other = (Column) obj;
        if (this.size != other.size) {
            return false;
        }
        if (this.nullable != other.nullable) {
            return false;
        }
        if (this.primaryKey != other.primaryKey) {
            return false;
        }
        if (this.autoIncrement != other.autoIncrement) {
            return false;
        }
        if (this.unique != other.unique) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.type, other.type)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 17 * hash + Objects.hashCode(this.name);
        hash = 17 * hash + Objects.hashCode(this.type);
        hash = 17 * hash + this.size;
        hash = 17 * hash + (this.nullable ? 1 : 0);
        hash = 17 * hash + (this.primaryKey ? 1 : 0);
        hash = 17 * hash + (this.autoIncrement ? 1 : 0);
        hash = 17 * hash + (this.unique ? 1 : 0);
        return hash;
    }

    @Override
    public String toString() {
        return "Column{" + "name=" + name + ", type=" + type + ", size=" + size + ", nullable=" + nullable + ", primaryKey=" + primaryKey + ", autoIncrement=" + autoIncrement + ", unique=" + unique + '}';
    }

}
