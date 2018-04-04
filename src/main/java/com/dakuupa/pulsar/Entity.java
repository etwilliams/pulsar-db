package com.dakuupa.pulsar;

import com.dakuupa.pulsar.annotations.DbAutoIncrement;
import com.dakuupa.pulsar.annotations.DbIgnore;
import com.dakuupa.pulsar.annotations.DbNotNull;
import com.dakuupa.pulsar.annotations.DbPrimaryKey;

/**
 * Base DB entity class
 *
 * @author EWilliams Each field corresponds to a column in a table named for the Entity class. see
 * com.globalstar.android.db.annotations package for annotations. Also
 * @see AbstractDatabaseManager
 */
public abstract class Entity {

    @DbPrimaryKey
    @DbAutoIncrement
    @DbNotNull
    /**
     * id is the primary key for the table.
     */
    protected Integer id;
    
    @DbIgnore
    /**
     * persisted is where the object is written to the table or not
     */
    private boolean persisted = false;
    
    @DbIgnore
    public static final int INVALID_ID = -1;

    public int getId() {
        /*if (id == null){
            return INVALID_ID;
        }*/
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public boolean isPersisted() {
        return persisted;
    }

    public void setPersisted(boolean persisted) {
        this.persisted = persisted;
    }

}
