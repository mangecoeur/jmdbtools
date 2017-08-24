/*
 JMdbTools
 Copyright (c) 2013, J. Chambers, All rights reserved.

 This library is free software; you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public
 License as published by the Free Software Foundation; either
 version 3.0 of the License, or (at your option) any later version.

 This library is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 Lesser General Public License for more details.

 You should have received a copy of the GNU Lesser General Public
 License along with this library.
 */


package jmdbtools;

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.util.ExportUtil;
import com.healthmarketscience.sqlbuilder.CreateTableQuery;
import com.healthmarketscience.sqlbuilder.InsertQuery;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;

/**
 *
 *
 *
 * Date: 25/11/2013
 */
public class JMdbTools {

    public static void main(String[] args) {
        System.out.println("[INFO] Starting mdb file processing");
        Options options = createOptions();
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmd = parser.parse(options, args);

            JMdbTools tool = new JMdbTools(cmd);


            if (cmd.hasOption("s")) {
                tool.showStats();
            }

            //export to mysql
            if (cmd.hasOption("db")) {
                tool.exportSQL();
            }

            if (cmd.hasOption("e")) {
                tool.exportCSV();
            }


        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private static Options createOptions(){
        // create Options object
        Options options = new Options();

        // add t option
        //options.addOption("f", true, "input mdb file path");
        options.addOption("s", false, "show file stats");
        options.addOption("db", true, "export to database");
        options.addOption("csv", false, "export csv file");

        options.addOption("o", false, "overwrite existing tables");
        options.addOption("tp", true, "table prefix");
        options.addOption("u", true, "database username");
        options.addOption("p", true, "database password");

        return options;
    }

    public String filePath;
    public Database db;

    public String tablePrefix = "";
    public String dbUsername;
    public String dbPassword;
    public String dbUrl;
    public Boolean dbOverwrite = false;

    public JMdbTools(CommandLine cmd){
        processOptions(cmd);
        loadDB();
    }

    //replace MySQL reserved names with acceptable alternative
    private String fixColumnName(String columnName) {
        Map<String, String> reservedNames = new HashMap<String, String>();
        reservedNames.put("database", "dbase");

        if (reservedNames.containsKey(columnName)) {
            return reservedNames.get(columnName);
        } else {
            return columnName;
        }
    }

    private String fixTableName(String tableName) {
        if (tablePrefix.equalsIgnoreCase("")) {
            return tableName;
        } else {
            return tablePrefix + tableName;
        }
    }

    private void processOptions(CommandLine cmd){
        if (cmd.hasOption("tp")) {
            tablePrefix = cmd.getOptionValue("tp");
        }
        if (cmd.hasOption("db")) {
            dbUrl = cmd.getOptionValue("db");
        }
        if (cmd.hasOption("o")) {
            dbOverwrite = true;
        }
        if (cmd.hasOption("u")) {
            dbUsername = cmd.getOptionValue("u", null);
        }
        if (cmd.hasOption("p")) {
            dbPassword = cmd.getOptionValue("p", null);
        }
        List<String> argList = cmd.getArgList();
        if (argList.isEmpty()) {
            log("No file parameter given!", "error");
        } else {
            filePath = argList.get(argList.size() - 1);
        }
    }

    private void loadDB(){
        try {
            File file = new File(filePath);
            db = DatabaseBuilder.open(file);

            log("Loaded database from file: " + file.getName(), "info");
        } catch (IOException e) {
            log("Could not load MDB file!", "error");
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private void showStats() {
        Set<String> tableNames;
        try {
            tableNames = db.getTableNames();
            for(String tableName: tableNames) {
                Table table = db.getTable(tableName);

                log(tableName);
                log("Row count");
                System.out.println(table.getRowCount());

                for(Column column : table.getColumns()) {
                    String columnName = column.getName();
                    //Object value = row.get(columnName);
                    log("Column " + columnName + "(" + column.getType() + ") ", "info");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void exportCSV(){
        File dbFile = db.getFile();
        String path = dbFile.getPath() + "-csv";
        File dir = new File(path);
        if (dir.mkdir()) {
            System.out.println("[INFO] Exporting data to directory: " + dir.getAbsolutePath());

            ExportUtil.Builder builder = new ExportUtil.Builder(db);
            builder.setHeader(true); //enable column headers
            try {
                builder.exportAll(dir);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            System.out.println("[ERROR] directory not created");
        }
    }

    private void exportSQL(){
        try {
            Connection conn = getConnection();

            DbSchema tableSet = createDbSchema();

            createTables(tableSet, conn);

            insertAllData(tableSet, conn);
        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            //finally block used to close resources
            //conn.close();

        }
    }

    private Connection getConnection(){
        /*try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            log("No MySQL Driver");
        }*/

        Connection conn = null;
        try {
            Properties prop = new Properties();
            if (dbUsername != null) {
                prop.setProperty("user", dbUsername);
            }
            if (dbPassword != null) {
                prop.setProperty("password", dbPassword);
            }

            conn = DriverManager.getConnection(dbUrl, prop);
        } catch (SQLException e) {
            log("SQLException: " + e.getMessage(), "error");
            log("SQLState: " + e.getSQLState(), "error");
            log("VendorError: " + e.getErrorCode(), "error");
        }

        return conn;
    }

    private DbSchema createDbSchema(){
        DbSpec spec = new DbSpec();
        DbSchema schema = spec.addDefaultSchema();
        Set<DbTable> dbTables = new HashSet<DbTable>();

        try {
            Set<String> tableNames = db.getTableNames();

            for(String tableName: tableNames) {
                Table table = db.getTable(tableName);
                DbTable dbTable = createTableSchema(table, schema);
                dbTables.add(dbTable);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return schema;
    }

    private DbTable createTableSchema(Table table, DbSchema schema) {

        // add table with basic customer info
        DbTable customerTable = schema.addTable(fixTableName(table.getName()));

        for(Column column : table.getColumns()) {
            //System.out.println("[INFO] ADDING Column " + column.getName() + "(" + column.getType() + ")");
            int columnType = 0;
            try {
                columnType = column.getType().getSQLType();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            if (columnType == Types.DOUBLE) {
                //DOUBLE is not ANSI - should be DOUBLE PRECISION
                customerTable.addColumn(fixColumnName(column.getName()), "DOUBLE PRECISION", null);

            } else {
                customerTable.addColumn(fixColumnName(column.getName()), columnType, null);
            }
        }

        return customerTable;
    }

    private void createTables(DbSchema schema, Connection conn) throws SQLException {

        Statement stmt = conn.createStatement();
        for(DbTable dbTable : schema.getTables()) {
            String createTableSql = new CreateTableQuery(dbTable, true).validate().toString();
            if (dbOverwrite) {
                log("Dropping existing table", "warn");
                stmt.executeUpdate("DROP TABLE IF EXISTS " + dbTable.getName() + "");
                log("creating table:" + dbTable.getName(), "info");
                stmt.executeUpdate(createTableSql);
            } else {
                DatabaseMetaData meta = conn.getMetaData();
                ResultSet res = meta.getTables(null, null, dbTable.getName(), new String[] {"TABLE"});
                if (res.last()) {
                    //there are entries for "TABLE" with this name don't try to create table
                    log("Table already exists:" + dbTable.getName(), "info");

                } else {
                    log("creating table: \"" + dbTable.getName(), "\" info");
                    stmt.executeUpdate(createTableSql);
                }
            }
        }
    }

    private void insertAllData(DbSchema schema, Connection conn) throws IOException {

        for (String tableName: db.getTableNames()) {
            Table table = db.getTable(tableName);
            DbTable dbTable = schema.findTable(fixTableName(tableName));
            insertData(table, dbTable, conn);

        }
    }

    private int[] insertData(Table dataTable, DbTable dbTable, Connection conn){
        Statement stmt;
        int[] execStatus = new int[0];
        try {
            stmt = conn.createStatement();
            log("Creating Insert Statements:" + dbTable.getName(), "info");

            for (Row row : dataTable){
                InsertQuery insertQuery = new InsertQuery(dbTable);
                for (Map.Entry<String, Object> col : row.entrySet()) {
                    //We had to add crap to the column name, so have to muck about to get match

                    DbColumn column = dbTable.findColumn(fixColumnName(col.getKey()));

                    if (col.getValue() != null){
                        if(column.getTypeNameSQL().equalsIgnoreCase("DATE") || column.getTypeNameSQL().equalsIgnoreCase("DATETIME")){
                            java.sql.Timestamp sqlDate = new java.sql.Timestamp(((java.util.Date) col.getValue()).getTime());
                            //log(sqlDate.toString(), "debug");
                            insertQuery.addColumn(column, sqlDate);
                        } else {
                            insertQuery.addColumn(column, col.getValue());
                        }
                    }
                }
                stmt.addBatch(insertQuery.validate().toString());
            }
            log("Executing Insert", "info");

            execStatus = stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            log(e.getNextException().getMessage(), "error");
        }

        return execStatus;

    }

    private static void log(String message){
        log(message, "info");
    }

    private static void log(String message, String level){
        String prfx = "";
        if (level.equalsIgnoreCase("info")) {
            prfx = "[INFO] ";
        } else if (level.equalsIgnoreCase("error")) {
            prfx = "[ERROR] ";
        } else if (level.equalsIgnoreCase("debug")) {
            prfx = "[DEBUG] ";
        }
        else if (level.equalsIgnoreCase("warn")) {
            prfx = "[WARN] ";
        }
        System.out.println(prfx + message);
    }
}
