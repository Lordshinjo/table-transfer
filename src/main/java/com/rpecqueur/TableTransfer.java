package com.rpecqueur;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class TableTransfer {
    private String user;
    private String password;
    private String targetTable;
    private String baseQuery;
    private Map<String, DataSource> datasources;
    private DataSource targetDatasource;

    public TableTransfer(String user,
                         String password,
                         String targetConnectionString,
                         String targetTable,
                         String baseQuery) {
        this.user = user;
        this.password = password;
        this.targetTable = targetTable;
        this.baseQuery = baseQuery;
        this.datasources = new HashMap<>();
        this.targetDatasource = getDatasource(targetConnectionString);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.out.println("Should have 6 arguments, got " + args.length);
            System.out.println("Usage: java TableTransfer.jar sourceDataFile sourceQueryFile targetJdbc targetTable user password");
            System.out.println();
            System.out.println("- sourceDataFile contains a list of 'connectionString|customerId|clientId', like 'jdbc:mysql://[host]:[port]/|3|4");
            System.out.println("- sourceQueryFile contains the base query with '@cuid' and '@clid' as placeholders for customerId and clientId respectively");
            System.out.println("- targetJdbc is the jdbc connection string of the target db, like 'jdbc:mysql://[host]:[port]/");
            System.out.println("- targetTable is the name of the target table, that should already exist with the correct schema on the target db");
            System.out.println("- user is the username, common for all connections");
            System.out.println("- password is the password, common for all connections");
            System.exit(1);
        }

        String sourceDataFile = args[0];
        String sourceQueryFile = args[1];
        String targetJdbc = args[2];
        String targetTable = args[3];
        String user = args[4];
        String password = args[5];

        String baseQuery = String.join("\n", Files.readAllLines(Paths.get(sourceQueryFile), StandardCharsets.UTF_8));

        TableTransfer transfer = new TableTransfer(user, password, targetJdbc, targetTable, baseQuery);

        Files.readAllLines(Paths.get(sourceDataFile)).parallelStream().forEach(s -> {
            String[] splits = s.split("\\|");
            try {
                transfer.doTransfer(splits[0], splits[1], splits[2]);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void doTransfer(String sourceConnectionString, String customerId, String clientId) throws SQLException {
        DataSource source = getDatasource(sourceConnectionString);
        String query = baseQuery
                .replaceAll("@cuid", customerId)
                .replaceAll("@clid", clientId);

        try (Connection sourceConnection = source.getConnection();
             Statement sourceStatement = sourceConnection.createStatement();
             ResultSet resultSet = sourceStatement.executeQuery(query)) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            StringBuilder queryBuilder = new StringBuilder("INSERT INTO ")
                    .append(targetTable)
                    .append(" VALUES(");
            for (int i = 0; i < columnCount; ++i) {
                queryBuilder.append("?,");
            }
            queryBuilder.deleteCharAt(queryBuilder.length() - 1).append(");");
            String insertQuery = queryBuilder.toString();

            try (Connection targetConnection = targetDatasource.getConnection();
                 PreparedStatement targetStatement = targetConnection.prepareStatement(insertQuery)) {
                for (int i = 1; i <= columnCount; ++i) {
                    targetStatement.setObject(i, resultSet.getObject(i));
                }
                targetStatement.executeUpdate();
            }
        }
    }

    synchronized private DataSource getDatasource(String connectionString) {
        if (!datasources.containsKey(connectionString)) {
            ComboPooledDataSource ds = new ComboPooledDataSource();
            try {
                ds.setDriverClass(com.mysql.jdbc.Driver.class.getName());
            } catch (PropertyVetoException e) {
                throw new RuntimeException("Unable to initialize datasource for " + connectionString, e);
            }
            ds.setJdbcUrl(connectionString);
            ds.setUser(user);
            ds.setPassword(password);
            datasources.put(connectionString, ds);
        }
        return datasources.get(connectionString);
    }
}
