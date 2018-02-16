package com.dakuupa.pulsar.typeconverter.mysql;

import com.dakuupa.pulsar.typeconverter.AbstractTypeConverter;

/**
 *
 * @author etwilliams
 */
public abstract class MySQLTypeConverter<T> extends AbstractTypeConverter<T> {

    public static final String DB_TYPE_VARCHAR = "VARCHAR";
    public static final String DB_TYPE_TEXT = "TEXT";
    public static final String DB_TYPE_INTEGER = "INT";
    public static final String DB_TYPE_LONG = "BIGINT";
    public static final String DB_TYPE_DOUBLE = "DOUBLE";
    public static final String DB_TYPE_FLOAT = "FLOAT";
    public static final String DB_TYPE_BOOLEAN = "TINYINT";
    public static final String DB_TYPE_SMALLINT = "SMALLINT";
    public static final String DB_TYPE_REAL = "REAL";
    public static final String DB_TYPE_DATETIME = "DATETIME";
    public static final String DB_TYPE_BLOB = "BLOB";
    public static final Integer DB_DEFAULT_INT_SIZE = 11;
    public static final Integer DB_DEFAULT_BOOLEAN_SIZE = 1;

}
