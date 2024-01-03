package com.neo4j.query.database;

import com.neo4j.query.QueryRecord;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class SQLiteAdapter implements IStorageAdapter {

    private  Connection dbConnection ;
    private Map<String, Object> configuration ;

    private String getQuerySQL ;
    private String insertQuerySQL ;
    private String addStartRecordSQL ;
    private String addEndRecordSQL ;

    private PreparedStatement getQueryStmt ;
    private PreparedStatement insertQueryStmt ;
    private PreparedStatement addStartRecordStmt ;
    private PreparedStatement addEndRecordStmt ;
    @Override
    public void initialize(Map<String, Object> configuration) throws Exception {
        this.configuration = configuration ;
        getQuerySQL = configuration.get("getQueryId").toString() ;
        insertQuerySQL = configuration.get("insertQuery").toString() ;
        addStartRecordSQL = configuration.get("addStartRecord").toString() ;
        addEndRecordSQL = configuration.get("addEndRecord").toString() ;
        dbConnection = DriverManager.getConnection(configuration.get("databaseURI").toString()) ;
    }

    @Override
    public void setupStorage() {
        List<Object> initQueries = (List<Object>) configuration.get("initQueries") ;
        try {
            Statement stmt = dbConnection.createStatement() ;
            for (Object o : initQueries) {
                // create a new table
                stmt.execute((o.toString()));

            }
//            stmt.execute("PRAGMA synchronous = OFF") ;
//            stmt.execute("PRAGMA journal_mode = MEMORY") ;
            stmt.close();
            getQueryStmt = dbConnection.prepareStatement(getQuerySQL);
            insertQueryStmt = dbConnection.prepareStatement(insertQuerySQL, Statement.RETURN_GENERATED_KEYS);
            addStartRecordStmt = dbConnection.prepareStatement(addStartRecordSQL);
            addEndRecordStmt = dbConnection.prepareStatement(addEndRecordSQL);
            dbConnection.setAutoCommit(false);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

//    INSERT INTO (db_query_id, db_transaction_id, query_id, database,
//                 db_id, authenticatedUser, executedUser, elapsedTimeMs, pageFaults,
//                 pageHits, planning, allocatedBytes, client, start_timeStamp)
//    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
//    ON CONFLICT DO UPDATE SET start_timeStamp=excluded.start_timeStamp;

    @Override
    public void addQueryStart(QueryRecord record) {
        try {
            record.queryId = getQueryId(record.query, record.runtime) ;

            addStartRecordStmt.setLong(1,record.dbQueryId);
            addStartRecordStmt.setLong(2,record.dbTransactionId);
            addStartRecordStmt.setLong(3,record.queryId);
            addStartRecordStmt.setString(4, record.dabtabase);
            addStartRecordStmt.setString(5, record.dbId);
            addStartRecordStmt.setString(6, record.authenticatedUser);
            addStartRecordStmt.setString(7, record.executedUser);
            addStartRecordStmt.setLong(8,record.elapsedTimeMs);
            addStartRecordStmt.setLong(9,record.pageFaults);
            addStartRecordStmt.setLong(10,record.pageHits);
            addStartRecordStmt.setLong(11,record.planning);
            addStartRecordStmt.setLong(12,record.allocatedBytes);
            addStartRecordStmt.setString(13, record.client);
            addStartRecordStmt.setTimestamp(14, record.timeStamp);
            addStartRecordStmt.executeUpdate() ;
            addStartRecordStmt.clearParameters();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addQueryEnd(QueryRecord record) {
        try {
            record.queryId = getQueryId(record.query, record.runtime) ;
            addEndRecordStmt.setLong(1,record.dbQueryId);
            addEndRecordStmt.setLong(2,record.dbTransactionId);
            addEndRecordStmt.setLong(3,record.queryId);
            addEndRecordStmt.setString(4, record.dabtabase);
            addEndRecordStmt.setString(5, record.dbId);
            addEndRecordStmt.setString(6, record.authenticatedUser);
            addEndRecordStmt.setString(7, record.executedUser);
            addEndRecordStmt.setLong(8,record.elapsedTimeMs);
            addEndRecordStmt.setLong(9,record.pageFaults);
            addEndRecordStmt.setLong(10,record.pageHits);
            addEndRecordStmt.setLong(11,record.planning);
            addEndRecordStmt.setLong(12,record.allocatedBytes);
            addEndRecordStmt.setString(13, record.client);
            addEndRecordStmt.setString(14, record.server);
            addEndRecordStmt.setTimestamp(15, record.timeStamp);
            addEndRecordStmt.executeUpdate() ;
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private long getQueryId(String query, String runtime) {
        long qId = -1 ;
        try {
            getQueryStmt.setString(1,query);
            getQueryStmt.setString(2, runtime);
            ResultSet qr = getQueryStmt.executeQuery() ;
            if( qr.next() ) {
                qId = qr.getLong("id") ;
                return qId ;
            }
            qr.close();
            getQueryStmt.clearParameters();
            // We don't have the query in the database. So insert this query.
            insertQueryStmt.setString(1,query);
            insertQueryStmt.setString(2, runtime);
            insertQueryStmt.executeUpdate() ;
            ResultSet rs = insertQueryStmt.getGeneratedKeys() ;
            rs.next();
            qId = rs.getInt(1);
            rs.close();
            insertQueryStmt.clearParameters();
            return qId ;
        }catch (Exception e) {
            e.printStackTrace();
        }

        return -1 ;
    }

    @Override
    public void commit() {
        try {
            dbConnection.commit();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
