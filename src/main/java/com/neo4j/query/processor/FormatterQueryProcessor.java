package com.neo4j.query.processor;

import com.neo4j.query.QueryRecord;
import com.neo4j.query.database.IStorageAdapter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.StringTokenizer;

public class FormatterQueryProcessor implements QueryProcessor {
    private IStorageAdapter storageAdapter ;

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
    private Map<String, Object> configuration ;

    @Override
    public void initialize(Map<String, Object> configuration, IStorageAdapter storageAdapter) {
        this.configuration = configuration ;
        this.storageAdapter = storageAdapter ;
    }

    @Override
    public void processFile(File file) {
        try {
            System.out.println("Processing File : " + file.getAbsolutePath());
            int counter = 0 ;
            BufferedReader reader = new BufferedReader(new FileReader(file)) ;
            StringBuffer query = new StringBuffer() ;

            while (true) {
                String line = reader.readLine() ;
                if( line == null ) {
                    break ;
                }
                if( isStartOfQuery(line)) {
                    // Process the previous query.
                    if( query.length() > 0 ) {
                        QueryRecord record = readQueryLogEntry(query.toString());
                        if (record != null) {
                            if (record.isStartRecord) {
                                storageAdapter.addQueryStart(record);
                            } else {
                                storageAdapter.addQueryEnd(record);
                            }
                            if (counter % 50000 == 0) {
                                System.out.println(file + " :: " + new Date() + " :: index : " + counter);
                                storageAdapter.commit();
                            }
                            counter++;
                        }
                    }
                    query = new StringBuffer() ;
                    query.append(line) ;
                    query.append("\n") ;
                } else {
                    query.append(line) ;
                    query.append("\n") ;
                }
            }
            if( query.length() > 0 ) {
                QueryRecord record = readQueryLogEntry(query.toString()) ;
                if( record != null ) {
                    if (record.isStartRecord) {
                        storageAdapter.addQueryStart(record);
                    } else {
                        storageAdapter.addQueryEnd(record);
                    }
                }
            }

        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private QueryRecord readQueryLogEntry(String query) {
        QueryRecord record = new QueryRecord();
        String[] splitData = query.split(" - ") ;
        record.timeStamp = Timestamp.valueOf(splitData[0].substring(0, 19).replace('T', ' '));
        String s = null ;

        int curPart = 0 ;

        String current = splitData[curPart] ;
        if( current.contains("Query started:") ) {
            record.isStartRecord = true ;
        } else {
            record.isStartRecord = false ;
        }
        int nextIndex = -1 ;
        int index = current.indexOf("transaction id:") ;
        if( index > 0 ) {
            // First part contais the Transaciton ID. This means we might not have query id.
            s = getKeyValue(current, "transaction id:", ' ') ;
            if( s != null ) {
                record.dbTransactionId = Long.valueOf(s.trim()) ;
            } else {
                record.dbTransactionId = -1 ;
            }
        } else {
            // First part foes not contain Transaction id.
            s = getKeyValue(current, "id:", ' ') ;
            if( s != null ) {
                record.dbQueryId = Long.valueOf(s.trim()) ;
            } else {
                record.dbQueryId = -1 ;
            }
        }
        curPart++ ;
        current = splitData[curPart] ;
        if( current.contains("transaction id:")) {
            s = getKeyValue(current, "transaction id:", ' ') ;
            if( s != null ) {
                record.dbTransactionId = Long.valueOf(s.trim()) ;
            } else {
                record.dbTransactionId = -1 ;
            }
            curPart++ ;
            current = splitData[curPart] ;
        }
        current = current.trim() ;
        index = current.indexOf(' ') ;
        if( index > 0 ) {
            s = current.substring(0,index).trim() ;
            record.elapsedTimeMs = Long.valueOf(s) ;
            if( record.elapsedTimeMs == 0 ) {
                record.elapsedTimeMs = 1 ;
            }
        }

        s = getKeyValue(current, "planning:", ',') ;
        if( s != null ) {
            record.planning = Long.valueOf(s.trim()) ;
        }
        s = getKeyValue(current, "cpu:", ' ') ;
        if( s != null ) {
            record.cpuTime = Long.valueOf(s.trim()) ;
        } else {
            s = getKeyValue(current, "cpu:", ')') ;
            if( s != null ) {
                record.cpuTime = Long.valueOf(s.trim()) ;
            }
        }
        s = getKeyValue(current, "waiting:", ')') ;
        if( s != null ) {
            record.waiting = Long.valueOf(s.trim()) ;
        }

        // Process Bytes
        curPart++ ;
        current = splitData[curPart].trim() ;
        index = current.indexOf(' ') ;
        record.allocatedBytes = Long.valueOf(current.substring(0,index)) ;

        // Process Page Hits and Page Faults
        curPart++ ;
        current = splitData[curPart].trim() ;
        index = current.indexOf(' ') ;
        record.pageHits = Long.valueOf(current.substring(0,index)) ;
        index = current.indexOf(',') ;
        nextIndex = current.indexOf(' ', index+2) ;
        s = current.substring(index+2,nextIndex) ;
        record.pageFaults = Long.valueOf(s) ;

        // Process Source Data
        curPart++ ;
        current = splitData[curPart].trim() ;

        if( current.startsWith("embedded-session")) {
            record.client = "embedded-session" ;
            record.authenticatedUser = current.substring(16).trim() ;
            record.executedUser = record.authenticatedUser;
        } else {
            StringTokenizer tokens = new StringTokenizer(current, "\\\t") ;
            String session = tokens.nextToken() ;
            if( session.equals("bolt-session")) {
                tokens.nextToken() ;

                String driverTxt = tokens.nextToken() ;
                int iindex = driverTxt.indexOf("/") ;
                if( iindex > 0 ) {
                    record.driver = driverTxt.substring(0,iindex) ;
                    record.driverVersion = driverTxt.substring(iindex+1) ;
                }
                record.client = getKeyValue(tokens.nextToken(), "client/", ':') ;
                record.server = getKeyValue(tokens.nextToken(), "server/", ':') ;
            } else {
                System.out.println("Unknown Session : " + current);
            }
        }
//        if( current.contains("bolt-session")) {
//            // Bolt Sesstion.
//            record.server = getKeyValue(current, "server/", ':') ;
//            record.client = getKeyValue(current, "client/", ':') ;
//            index = current.lastIndexOf(' ') ;
//            if( index == -1 ) {
//                index = current.lastIndexOf('\t') ;
//            }
//            if( index != -1 ) {
//                record.dabtabase = current.substring(index+1).trim() ;
//            }
//        }

        curPart++ ;
        if( record.client!= null && !record.client.equals("embedded-session")) {
            record.authenticatedUser = splitData[curPart] ;
            record.executedUser = splitData[curPart] ;
        }

        curPart++ ;
        // Process the runtime and skip metadata and parameters
        // Also set the last part location.
        int len = splitData.length ;
        int lastPart ;
        s = splitData[len-2].trim() ;
        if( s.startsWith("runtime=")) {
            record.runtime = s.substring(8).trim() ;
            if( record.runtime != null && record.runtime.equals("null")) {
                record.runtime = null ;
            }
            lastPart = len - 2 ;
        } else if(splitData[len-3].trim().startsWith("runtime=")){
            // We have error at the end.
            record.stackTrace = splitData[len-1].trim() ;
            record.failed = 1 ;
            lastPart = len - 3 ;
        } else {
            lastPart = len - 1 ;
        }
        // Skip any parameters that have the same pattern as " - "
        while (true) {
            if( splitData[lastPart-1].startsWith("{")) {
                break;
            }
            lastPart-- ;
        }
        // Get the Query
        StringBuffer queryStringBuffer = new StringBuffer() ;
        for( int i = curPart ; i < lastPart-1 ; i++ ) {
            queryStringBuffer.append(splitData[i]) ;
        }
        record.query = queryStringBuffer.toString().trim() ;

        return record ;
    }


    public void finishProcesing() {
        try {
            // Finish out committing any remaining transactions
            storageAdapter.commit();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String cleanQuery(String query) {
        String modified = query.toLowerCase() ;
        if( modified.startsWith("cypher ")) {
            // We need to take out first 2 words.

            int index = findFirstWhiteSpace(query, 7) ;
            if( index > 0 ) {
                modified = query.substring(index+1) ;
                return modified ;
            }
        }
        return query ;
    }
    private int findFirstWhiteSpace(String query, int start) {
        int len = query.length() ;
        for( int i = start; i < len ; i++ ) {
            char c = query.charAt(i) ;
            if( c == ' ' || c == '\t' || c == '\n' ) {
                return i ;
            }
        }
        return -1 ;
    }

    private static String getKeyValue(String part, String key, char delimiter) {
        String retValue = null ;

        int index = part.indexOf(key) ;
        if( index != -1 ) {
            int nextIndex = part.indexOf(delimiter, index+key.length()) ;
            if( nextIndex > 0 ) {
                retValue = part.substring(index + key.length(), nextIndex);
            } else {
                retValue = part.substring(index + key.length(), part.length());
            }
        }

        return retValue ;
    }

//    private void parseQueryString(String s) {
//        boolean start = isStartOfQuery(s) ;
//        System.out.println("Done : " + start);
//        if( start ) {
//            int index = s.indexOf(' ', 11) ;
//            Timestamp sts = Timestamp.valueOf(s.substring(0, 19).replace('T', ' '));
//
//            index = s.indexOf(" ms:", index) ;
//            int prevIndex = s.substring(0,index).lastIndexOf(' ') ;
//            String time = s.substring(prevIndex, index) ;
//            System.out.println("Time : " + time.trim());
//            System.out.println("Done : ");
//        }
//
//
//    }

    private boolean isStartOfQuery(String query) {
        int index = query.indexOf(' ', 11) ;
        if( index != -1 ) {
            try {
                String s = query.substring(0, index);
                if( s.length() < 19 ) {
                    return false ;
                }
                String timestamp = s.substring(0, 19).replace('T', ' ');
                Timestamp sts = null ;
                try {
                    sts = Timestamp.valueOf(timestamp);
                }catch (Exception e) {
                    sts = null ;
                }
                if( sts != null ) {
                    //System.out.println("Query starts with Time stamp"  + sts);
                    int nextIndex = query.indexOf(' ', index+1) ;
                    s = query.substring(index+1, nextIndex) ;
                    //System.out.println("Next Value : " + s);
                    if( s.equals("INFO") || s.equals("ERROR") || s.equals("DEBUG") || s.equals("WARN")) {
                        return true;
                    }
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        return false ;
    }

//    private Map<String, Object> processFile(BufferedReader reader, String lastRead) {
//        StringBuffer query = new StringBuffer() ;
//        int nextIndex = index ;
//        Map<String, Object> returnData = new HashMap<>() ;
//
//        query.append(data[nextIndex++]) ;
//        while (true) {
//            String s = data[nextIndex] ;
//            if( isStartOfQuery(s) ) {
//                returnData.put("query", query) ;
//                returnData.put("index", nextIndex) ;
//                break;
//            } else {
//                query.append("\n") ;
//                query.append(s) ;
//                nextIndex++ ;
//            }
//
//        }
//        return returnData ;
//    }

}
