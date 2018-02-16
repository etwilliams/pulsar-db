package com.dakuupa.pulsar.test;

import com.dakuupa.pulsar.AbstractDatabaseManager;
import com.dakuupa.pulsar.QueryArguments;
import java.sql.Connection;
import java.util.List;

public class DemoManager extends AbstractDatabaseManager<Demo> {

    public DemoManager(Connection con) {
        super(con);
    }

    public List<Demo> findByUUID(String uuid) {
        QueryArguments args = new QueryArguments();
        args.add("uuid", uuid);
        return listWhereArgsEquals(args);
    }

}
