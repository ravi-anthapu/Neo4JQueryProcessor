package com.neo4j.query;

import com.neo4j.query.database.IQueryTypeDefinition;

import java.sql.Timestamp;

public class QueryRecord {
    public long dbQueryId ;
    public long dbTransactionId ;
    public long queryId ;
    public String dabtabase ;
    public String dbId ;
    public String authenticatedUser ;
    public String executedUser ;
    public long elapsedTimeMs ;
    public long pageFaults ;
    public long pageHits ;
    public long planning ;
    public long cpuTime ;
    public long waiting ;
    public Timestamp timeStamp ;
    public long allocatedBytes ;
    public String driver ;
    public String driverVersion ;
    public String client ;
    public String server ;
    public String query ;
    public int queryType = IQueryTypeDefinition.APP;
    public String runtime ;
    public boolean isStartRecord ;
    public int failed = 0 ;
    public String stackTrace ;
}
