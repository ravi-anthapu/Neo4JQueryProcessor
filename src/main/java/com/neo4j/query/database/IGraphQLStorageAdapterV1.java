package com.neo4j.query.database;

import com.neo4j.query.QueryRecord;

import java.sql.Connection;
import java.util.Map;

public interface IGraphQLStorageAdapterV1 {
    public void initialize(Map<String, Object> configuration, Connection dbConnection) throws Exception;
    public void setupStorage() ;
    public void addGraphQLAnnotation(QueryRecord record) ;
}

