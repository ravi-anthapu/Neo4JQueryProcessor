package com.neo4j.query.database;

import com.neo4j.query.QueryRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class SQLiteGraphQLStorageAdapterV1 implements IGraphQLStorageAdapterV1 {
    private Connection dbConnection ;
    private Map<String, Object> dbConfiguration ;

    private PreparedStatement addAnnotationStmt ;
    private PreparedStatement getAnnotationStmt ;
    private PreparedStatement addAnnotationParamsStmt ;
    private PreparedStatement getParamsIdStmt ;
    private PreparedStatement getQueryIdStmt ;
    private PreparedStatement insertQueryStmt ;

    @Override
    public void initialize(Map<String, Object> configuration, Connection dbConnection) throws Exception {
        this.dbConnection = dbConnection ;
        this.dbConfiguration = configuration ;
    }

    @Override
    public void setupStorage() {
        List<Object> initQueries = (List<Object>) dbConfiguration.get("initQueries") ;
        try {
            Statement stmt = dbConnection.createStatement();
            for (Object o : initQueries) {
                // create a new table
                stmt.execute((o.toString()));

            }
            stmt.close();
            addAnnotationStmt = dbConnection.prepareStatement(dbConfiguration.get("addAnnotation").toString());
            getAnnotationStmt = dbConnection.prepareStatement(dbConfiguration.get("getAnnotation").toString());
            addAnnotationParamsStmt = dbConnection.prepareStatement(dbConfiguration.get("addAnnotationParams").toString());
            getParamsIdStmt = dbConnection.prepareStatement(dbConfiguration.get("getParamsId").toString());
            getQueryIdStmt = dbConnection.prepareStatement(dbConfiguration.get("getQueryId").toString());
            insertQueryStmt = dbConnection.prepareStatement(dbConfiguration.get("insertQuery").toString());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addGraphQLAnnotation(QueryRecord record) {
        if( record.annotationData != null ) {
            if( record.annotationData.get("annotationData.source.query") == null || record.annotationData.get("annotationData.app") == null ) {
                return;
            }
            String query = record.annotationData.get("annotationData.source.query").toString() ;
            String app = record.annotationData.get("annotationData.app").toString() ;
            long queryId = record.queryId ;
            long graphQlQueryId = -1 ;
            try {
                getQueryIdStmt.setString(1, query);
                getQueryIdStmt.setString(2, app);
                ResultSet qr = getQueryIdStmt.executeQuery() ;
                if( qr.next() ) {
                    graphQlQueryId = qr.getLong("id") ;
                }
                qr.close();
                getQueryIdStmt.clearParameters(); ;

                if( graphQlQueryId == -1 ) {
                    insertQueryStmt.setString(1, query);
                    insertQueryStmt.setString(2, app);

                    insertQueryStmt.executeUpdate() ;
                    ResultSet rs = insertQueryStmt.getGeneratedKeys() ;
                    rs.next();
                    graphQlQueryId = rs.getInt(1);
                    rs.close();
                    insertQueryStmt.clearParameters();
                }

                boolean found = false ;
                getAnnotationStmt.setLong(1, graphQlQueryId);
                getAnnotationStmt.setLong(2, queryId);
                ResultSet rs = getAnnotationStmt.executeQuery() ;
                if( rs.next() ) {
                    found = true ;
                }
                rs.close();
                getAnnotationStmt.clearParameters();

                if( !found ) {
                    addAnnotationStmt.setLong(1, graphQlQueryId);
                    addAnnotationStmt.setLong(2, queryId);
                    addAnnotationStmt.executeUpdate() ;
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
