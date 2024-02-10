package com.neo4j.query.processor;

import com.neo4j.query.database.IStorageAdapter;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

public interface QueryProcessor {
    public void initialize(Map<String, Object> configuration, IStorageAdapter storageAdapter) ;
    public void processFile(String fileName, InputStream is) ;

    public void finishProcesing() ;
}
