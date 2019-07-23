package com.dakuupa.pulsar;

import com.dakuupa.pulsar.annotations.DbAutoIncrement;
import com.dakuupa.pulsar.annotations.DbIgnore;
import com.dakuupa.pulsar.annotations.DbNotNull;
import com.dakuupa.pulsar.annotations.DbPrimaryKey;
import com.dakuupa.pulsar.annotations.DbSize;

/**
 * Base DB entity class
 *
 * @author EWilliams Each field corresponds to a column in a table named for the
 * Entity class.
 * @see com.dakuupa.pulsar.db.annotations package for annotations
 * @see com.dakuupa.pulsar.db.AbstractDatabaseManager
 */
public abstract class Entity {

    @DbPrimaryKey
    @DbAutoIncrement
    @DbNotNull
    @DbSize(size = 11)
    /**
     * id is the primary key for the table.
     */
    protected Long id;

    @DbIgnore
    /**
     * persisted is where the object is written to the table or not
     */
    private boolean persisted = false;

    @DbIgnore
    public static final long INVALID_ID = -1;

    public int getId() {
        return Integer.parseInt(id+"");
    }

    public Long getIdLong() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public boolean isPersisted() {
        return persisted;
    }

    public void setPersisted(boolean persisted) {
        this.persisted = persisted;
    }

}
