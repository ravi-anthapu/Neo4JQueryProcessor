package com.neo4j.query.processor;

import java.io.File;
import java.util.Map;

public interface QueryProcessor {
    public void initialize(Map<String, Object> configuration) ;
    public void processFile(File file) ;

    public void finishProcesing() ;
}
