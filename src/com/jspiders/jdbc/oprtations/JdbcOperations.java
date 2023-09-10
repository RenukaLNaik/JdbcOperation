package com.jspiders.jdbc.oprtations;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;

public class JdbcOperations {

    private static Connection connection;
    private static java.sql.Statement statement;
    private static int result;
    private static ResultSet resultSet;
    private static Properties properties = new Properties();
    private static FileInputStream file;
    private static String filePath = "E:\\WEJA2\\jdbc\\resources\\db_info.properties";
    private static String query;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        // 1. Create a table
        try {
            openConnection();
            query = "CREATE TABLE emp ("
                    + "empno INT,"
                    + "ename VARCHAR(45) NOT NULL,"
                    + "job VARCHAR(45) ,"
                    + "mrg INT,"
                    + "hiredate DATE NOT NULL,"
                    + "sal INT NOT NULL,"
                    + "comm INT,"
                    + "PRIMARY KEY (empno))";
            result = ((java.sql.Statement) statement).executeUpdate(query);
            System.out.println("Query OK, " + result
                    + " row's affected.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        // 2. Insert 14 records
        try {
            openConnection();
            for (int i = 1; i <= 2; i++) {
                System.out.println("Enter empno in number :");
                int empno = scanner.nextInt();
                System.out.println("Enter ename String :");
                String ename = scanner.next();
                System.out.println("Enter job String :");
                String job = scanner.next();
                System.out.println("Enter mrg number :");
                int mrg = scanner.nextInt();
                System.out.println("Enter hiredate in this YYYY-MM-DD formate :");
                String hiredate = scanner.next();
                System.out.println("Enter sal number :");
                int sal = scanner.nextInt();
                System.out.println("Enter comm number:");
                int comm = scanner.nextInt();
                query = "INSERT INTO emp " + "VALUES (" + empno + ",'"
                        + ename + "','" + job + "'," + mrg + ",'" + hiredate + "',"
                        + sal + "," + comm + ")";
                result = result + ((java.sql.Statement) statement).executeUpdate(query);
            }
            System.out.println("Query OK, " + result
                    + " row's affected.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        // 3. Select all records
        selectAllRecords();
        // 4. Update 2 records
        try {
            openConnection();
            for (int i = 1; i <= 1; i++) {
                System.out.println("Enter the empno to update record:");
                int empnoToUpdate = scanner.nextInt();

                // Prompt for the fields to update
                System.out.println("Enter newEname String :");
                String newEname = scanner.next();
                System.out.println("Enter newJob String :");
                String newJob = scanner.next();
                System.out.println("Enter newMrg number :");
                int newMrg = scanner.nextInt();
                System.out.println("Enter newHiredate in this YYYY-MM-DD formate :");
                String newHiredate = scanner.next();
                System.out.println("Enter newSal number :");
                int newSal = scanner.nextInt();
                System.out.println("Enter newComm number:");
                int newComm = scanner.nextInt();

                query = "UPDATE emp SET ename = '" + newEname + "', job = '" + newJob + "', mrg = " + newMrg
                        + ", hiredate = '" + newHiredate + "', sal = "
                        + newSal + ", comm = " + newComm + " WHERE empno = " + empnoToUpdate;
                result = result + ((java.sql.Statement) statement).executeUpdate(query);
            }
            System.out.println("Query OK, " + result + " row's affected.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        // 5. Select all records
        selectAllRecords();
        // 6. Delete 3 records with fixed values
        try {
            openConnection();
            for (int i = 1; i <= 1; i++) {
                System.out.println("Enter the empno to delete record " + i + ":");
                int empnoToDelete = scanner.nextInt();
                query = "DELETE FROM emp WHERE empno = " + empnoToDelete;
                result = ((java.sql.Statement) statement).executeUpdate(query);
                System.out.println("Query OK, " + result + " row(s) affected.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
        // 7. Select all records
        selectAllRecords();
        scanner.close();
    }

    private static void openConnection() {
        try {
            file = new FileInputStream(filePath);
            properties.load(file);
            Class.forName(properties.getProperty("driverPath"));
            connection = DriverManager.getConnection(properties.getProperty("dburl"), properties);
            statement = connection.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void closeConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
            if (file != null) {
                file.close();
            }
            if (result != 0) {
                result = 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void selectAllRecords() {
        try {
            openConnection();
            String query = "SELECT * FROM emp";
            ResultSet resultSet = statement.executeQuery(query);
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1) + " | "
                        + resultSet.getString(2) + " | "
                        + resultSet.getString(3) + " | "
                        + resultSet.getString(4) + " | "
                        + resultSet.getString(5) + " | "
                        + resultSet.getString(6) + " | "
                        + resultSet.getString(7));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

}
