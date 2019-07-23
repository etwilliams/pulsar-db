/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dakuupa.pulsar;

/**
 *
 * @author Eric
 */
public class Settings {

    private String dbName;
    private String host;
    private String user;
    private String password;
    private String port;
    
    public Settings(String dbName, String host, String user, String password, String port) {
        this.dbName = dbName;
        this.host = host;
        this.user = user;
        this.password = password;
        this.port = port;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }
}