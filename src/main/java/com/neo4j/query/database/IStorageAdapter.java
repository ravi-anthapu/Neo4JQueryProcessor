package com.neo4j.query.database;

import com.neo4j.query.QueryRecord;

import java.util.Map;

public interface IStorageAdapter {
    public void initialize(Map<String, Object> configuration) throws Exception;
    public void setupStorage() ;

    public void addQueryStart(QueryRecord record) ;

    public void addQueryEnd(QueryRecord record) ;

    public void commit() ;

}
