package com.neo4j.query.database;

import com.neo4j.query.QueryRecord;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class SQLiteAdapter implements IStorageAdapter {

    private  Connection dbConnection ;
    private Map<String, Object> configuration ;
    private Map<String, Object> dbConfiguration ;
    private Map<String, Object> annotationConfiguration ;

    private String getQuerySQL ;
    private String updateQueryRuntimeSQL ;
    private String insertQuerySQL ;
    private String addStartRecordSQL ;
    private String addEndRecordSQL ;

    private PreparedStatement getQueryStmt ;
    private PreparedStatement updateQueryRuntimeStmt ;
    private PreparedStatement insertQueryStmt ;
    private PreparedStatement addStartRecordStmt ;
    private PreparedStatement addEndRecordStmt ;
    private int dummyTransactionId = -1 ;
    private IGraphQLStorageAdapterV1 graphQLAnnotationStorage ;

    @Override
    public void initialize(Map<String, Object> configuration) throws Exception {
        this.configuration = configuration ;
        Yaml yaml = new Yaml();
        try {
            InputStream inputStream = this.getClass()
                    .getClassLoader()
                    .getResourceAsStream("sqlite.yaml");
            dbConfiguration = yaml.load(inputStream);

            if( configuration.get("annotaionStorageConfig") != null ){
                InputStream annotationIs = this.getClass()
                        .getClassLoader()
                        .getResourceAsStream(configuration.get("annotaionStorageConfig").toString());

                annotationConfiguration = yaml.load(annotationIs);
            }

            getQuerySQL = dbConfiguration.get("getQueryId").toString();
            updateQueryRuntimeSQL = dbConfiguration.get("updateQueryRuntime").toString() ;
            insertQuerySQL = dbConfiguration.get("insertQuery").toString();
            addStartRecordSQL = dbConfiguration.get("addStartRecord").toString();
            addEndRecordSQL = dbConfiguration.get("addEndRecord").toString();
        }catch (Exception e) {
            e.printStackTrace();
        }
        dbConnection = DriverManager.getConnection(configuration.get("databaseURI").toString()) ;
        Object annotationEnabled = configuration.get("annotationProcessingEnabled") ;
        if( annotationEnabled != null && annotationEnabled.toString().equals("true") && annotationConfiguration != null ) {
            Object annotationType = annotationConfiguration.get("annotationType") ;
            Object annotationClass = annotationConfiguration.get("annotationClass") ;
            if( annotationType != null && annotationType.toString().equals("graphql")) {
                Class<IGraphQLStorageAdapterV1> adapterClass = (Class<IGraphQLStorageAdapterV1>) Class.forName(annotationClass.toString()) ;
                Constructor<IGraphQLStorageAdapterV1> ctor = adapterClass.getConstructor();
                graphQLAnnotationStorage = ctor.newInstance() ;
                graphQLAnnotationStorage.initialize(annotationConfiguration, dbConnection);
            }
        }

    }

    @Override
    public void setupStorage() {
        List<Object> initQueries = (List<Object>) dbConfiguration.get("initQueries") ;
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
            updateQueryRuntimeStmt = dbConnection.prepareStatement(updateQueryRuntimeSQL) ;
            insertQueryStmt = dbConnection.prepareStatement(insertQuerySQL, Statement.RETURN_GENERATED_KEYS);
            addStartRecordStmt = dbConnection.prepareStatement(addStartRecordSQL);
            addEndRecordStmt = dbConnection.prepareStatement(addEndRecordSQL);

            if( graphQLAnnotationStorage != null ) {
                graphQLAnnotationStorage.setupStorage();
            }

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
            setQueryType(record) ;
            if( record.dbTransactionId < 0 && record.dbQueryId == 0 ) {
                record.dbTransactionId = dummyTransactionId-- ;
            }
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
            addStartRecordStmt.setString(14, record.server);
            addStartRecordStmt.setTimestamp(15, record.timeStamp);
            addStartRecordStmt.setInt(16, record.failed);
            String s = record.stackTrace ;
            if( s != null && s.length() > 4000 ) {
                s = s.substring(0,4000) ;
            }
            addStartRecordStmt.setString(17, s);
            addStartRecordStmt.setString(18, record.driver);
            addStartRecordStmt.setString(19, record.driverVersion);
            addStartRecordStmt.setInt(20, record.queryType);
            addStartRecordStmt.executeUpdate() ;
            addStartRecordStmt.clearParameters();
            if( graphQLAnnotationStorage != null ) {
                graphQLAnnotationStorage.addGraphQLAnnotation(record);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addQueryEnd(QueryRecord record) {
        try {
            record.queryId = getQueryId(record.query, record.runtime) ;
            setQueryType(record) ;
            if( record.dbTransactionId < 0 && record.dbQueryId == 0 ) {
                record.dbTransactionId = dummyTransactionId-- ;
            }
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
            long adjust = ( (record.elapsedTimeMs/1000) + ((record.elapsedTimeMs%1000)>0?1:0) )*1000 ;
            addEndRecordStmt.setTimestamp(15, new Timestamp(record.timeStamp.getTime()-adjust));
            addEndRecordStmt.setTimestamp(16, record.timeStamp);
            addEndRecordStmt.setInt(17, record.failed);
            String s = record.stackTrace ;
            if( s != null && s.length() > 4000 ) {
                s = s.substring(0,4000) ;
            }
            addEndRecordStmt.setString(18, s);
            addEndRecordStmt.setString(19, record.driver);
            addEndRecordStmt.setString(20, record.driverVersion);
            addEndRecordStmt.setInt(21, record.queryType);
            addEndRecordStmt.executeUpdate() ;
            addEndRecordStmt.clearParameters();
            if( graphQLAnnotationStorage != null ) {
                graphQLAnnotationStorage.addGraphQLAnnotation(record);
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setQueryType(QueryRecord record) {
        //CALL dbms.routing.getRoutingTable($routingContext, $databaseName)
        if( record.query != null && record.query.contains("dbms.")) {
            if( record.query.contains("dbms.routing") ) {
                record.queryType = IQueryTypeDefinition.DBMS_ROUTING;
            } else {
                record.queryType = IQueryTypeDefinition.DBMS;
            }
            return;
        }
        if( record.driver != null && record.driver.equalsIgnoreCase("neo4j-browser")) {
            record.queryType = IQueryTypeDefinition.Browser ;
        }
    }

    private long getQueryId(String query, String runtime) {
        long qId = -1 ;
        try {
            String s = query.toLowerCase() ;
            int writeQuery = 0 ;
            if( s.contains("create ") || s.contains("merge ") || s.contains("delete ") || s.contains("set ") || s.contains("remove ")) {
                writeQuery = 1 ;
            }

            if( runtime != null && runtime.equals("null")) {
                runtime = null ;
            }

            getQueryStmt.setString(1,query);
//            getQueryStmt.setString(2, runtime);
//            getQueryStmt.setString(3, runtime);
            ResultSet qr = getQueryStmt.executeQuery() ;
            while ( qr.next() ) {
                qId = qr.getLong("id") ;
                String dbRuntime = qr.getString("runtime") ;
                if( dbRuntime == null && runtime != null ) {
                    // Update runtime if it is null.
                    try {
                        updateQueryRuntimeStmt.setString(1, runtime);
                        updateQueryRuntimeStmt.setLong(2, qId);
                        updateQueryRuntimeStmt.executeUpdate();
                        updateQueryRuntimeStmt.clearParameters();
                    }catch (Exception e) {
                        // Ignore update failure.
                        e.printStackTrace();
                    }
                }
                if( dbRuntime != null && runtime != null && !dbRuntime.equals(runtime)) {
                    qId = -1 ;
                }
            }
            qr.close();
            getQueryStmt.clearParameters();
            if( qId != -1 ) {
                return qId ;
            }
            // We don't have the query in the database. So insert this query.
            insertQueryStmt.setString(1,query);
            insertQueryStmt.setString(2, runtime);
            insertQueryStmt.setInt(3, writeQuery);
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
