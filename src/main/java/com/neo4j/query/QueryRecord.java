package com.neo4j.query;

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
    public long waiting ;
    public Timestamp timeStamp ;
    public long allocatedBytes ;
    public String client ;
    public String query ;
    public String runtime ;
    public boolean isStartRecord ;
}
