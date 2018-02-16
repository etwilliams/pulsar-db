package com.dakuupa.pulsar.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Random;

/**
 *
 * @author etwilliams
 */
public class Main {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/satfi", "globalstar", "g!oba!st*r1");

        DemoManager manager = new DemoManager(connection);
        
        Demo demo = new Demo();
        Random ran = new Random();
        demo.setiValue(ran.nextInt(1000)+1);
        demo.setDvalue(10.1);
        demo.setFvalue(123.123f);
        demo.setBvalue(true);
        demo.setLongStrValue("Mississippi Mississippi Mississippi Mississippi Mississippi Mississippi Mississippi Mississippi Mississippi Mississippi");
        demo.setUniqueStr(ran.nextInt(1000)+1+"");
        
        manager.insert(demo);
        
        for (Demo obj : manager.list()) {
            System.out.println(obj.toString());
            obj.setiValue(obj.getiValue()+1);
            manager.update(obj);
        }

    }

}
