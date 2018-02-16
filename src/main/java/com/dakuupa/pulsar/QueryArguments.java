package com.dakuupa.pulsar;

import java.util.HashMap;

/**
 * Class to contain query arguments
 *
 * @author EWilliams
 *
 */
public class QueryArguments {

    private HashMap<String, Object> args = new HashMap<>();

    public void add(String key, Object value) {

        //handle boolean to small int conversion for DB
        if (value instanceof Boolean) {
            if ((Boolean) value == true) {
                value = 1;
            } else {
                value = 0;
            }
        }

        args.put(key, value);
    }

    public HashMap<String, Object> getArgs() {
        return args;
    }

}
