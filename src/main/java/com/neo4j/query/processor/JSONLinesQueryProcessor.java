package com.neo4j.query.processor;

import com.neo4j.query.QueryRecord;
import com.neo4j.query.database.IStorageAdapter;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class JSONLinesQueryProcessor implements QueryProcessor {
    private IStorageAdapter storageAdapter ;

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
    private Map<String, Object> configuration ;

    private String serverHostNameKeys ;

    private String dbIdFilter ;

    @Override
    public void initialize(Map<String, Object> configuration, IStorageAdapter storageAdapter) {
        this.configuration = configuration ;
        this.storageAdapter = storageAdapter ;
        this.serverHostNameKeys = (String) configuration.get("server_host_name") ;
        this.dbIdFilter = (String) configuration.get("dbid") ;
    }

    @Override
    public void processFile(String fileName, InputStream is) {
        try {
            System.out.println("Processing File : " + fileName);
            int counter = 0 ;

            ObjectMapper mapper = new ObjectMapper() ;

            BufferedReader reader = new BufferedReader(new InputStreamReader(is)) ;
            String line = null ;
            line = reader.readLine() ;
            while( line != null ) {

                Map<String, Object> data = mapper.readValue(line, HashMap.class) ;
                Object type = data.get("type") ;
                if( type != null && type.toString().equals("transaction")) {
                    // Ignore the transaction only entries.
                    line = reader.readLine() ;
                    continue;
                }
                if(counter % 50000 == 0 ) {
                    System.out.println(fileName + " :: " + new Date() + " :: index : " + counter);
                    storageAdapter.commit();
                }
                counter++ ;

                QueryRecord record = readQueryLogEntry(data) ;
                if( record != null ) {
                    if (record.isStartRecord) {
                        storageAdapter.addQueryStart(record);
                    } else {
                        storageAdapter.addQueryEnd(record);
                    }
                }
                line = reader.readLine() ;
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private QueryRecord readQueryLogEntry(Map<String, Object> node) {
        QueryRecord record = null ;

        try {

            String timestamp = null ;
            String serverName = null ;
            if( serverHostNameKeys != null ) {
                String[] keysSequence = serverHostNameKeys.split("::") ;
                Map<String, Object> curMap = node ;
                for( int i = 0 ; i < keysSequence.length-1 ; i++ ) {
                    if( curMap != null && curMap.containsKey(keysSequence[i])) {
                        curMap = (Map<String, Object>) curMap.get(keysSequence[i]) ;
                    } else {
                        curMap = null ;
                    }
                }
                if( curMap != null ) {
                    if( curMap.containsKey(keysSequence[keysSequence.length-1])) {
                        serverName = curMap.get(keysSequence[keysSequence.length-1]).toString() ;
                    }
                }
            }
            if( node.containsKey("jsonPayload")) {
                // This is Aura log files downloaded as json lines.
                if( node.containsKey("timestamp")) {
                    // This is the Aura JSON log format that is exported by support.
                    timestamp = node.get("timestamp").toString().substring(0, 19).replace('T', ' ');
                } else if( node.containsKey("time")) {
                    // This is the JSON format of query log downloaded by customers.
                    timestamp = node.get("time").toString().substring(0, 19).replace('T', ' ');
                }

                node = (Map<String, Object>) node.get("jsonPayload") ;

            } else {
                // This is the JSON format of query log downloaded by customers.
//                timestamp = node.get("time").toString().substring(0, 19).replace('T', ' ');
                if( node.containsKey("timestamp")) {
                    // This is the Aura JSON log format that is exported by support.
                    timestamp = node.get("timestamp").toString().substring(0, 19).replace('T', ' ');
                } else if( node.containsKey("time")) {
                    // This is the JSON format of query log downloaded by customers.
                    timestamp = node.get("time").toString().substring(0, 19).replace('T', ' ');
                }
            }
            Timestamp sts = Timestamp.valueOf(timestamp);

            String dbId = node.containsKey("dbid")?node.get("dbid").toString():null;
            if( dbIdFilter != null && dbId != null ) {
                if( !dbIdFilter.equals(dbId)) {
                    // We don't want this database query logs.
                    return  null ;
                }
            }

            record = new QueryRecord();
            record.dbId = dbId ;
            record.serverHostName = serverName ;
            record.timeStamp = sts;

            readAnnotationData(record, node) ;

            String event = node.get("event").toString();
            if( event.equalsIgnoreCase("fail")) {
                record.failed = 1 ;
                record.stackTrace = node.containsKey("stacktrace")?node.get("stacktrace").toString():null ;
            }

            if (event.equalsIgnoreCase("start")) {
                record.isStartRecord = true;
            } else {
                record.isStartRecord = false;
            }

            record.dbQueryId = node.get("id")!=null?Long.valueOf(node.get("id").toString()):0;
            Object o = node.get("transactionId") ;
            if( o != null ) {
                record.dbTransactionId = Long.valueOf(o.toString());
            } else {
                o = node.get("transactionid") ;
                if( o != null ) {
                    record.dbTransactionId = Long.valueOf(o.toString());
                }
            }

            record.planning = node.containsKey("planning")?(Integer)node.get("planning"):0;
            Object tempKey = node.containsKey("elapsedTimeMs")?node.get("elapsedTimeMs"):node.get("elapsedtimems") ;
            record.elapsedTimeMs = (Integer) tempKey;

            if( record.elapsedTimeMs == 0 ) {
                record.elapsedTimeMs = 1 ;
            }

            tempKey = node.containsKey("pageFaults")?node.get("pageFaults"):node.get("pagefaults") ;
            record.pageFaults = (Integer)tempKey ;
            tempKey = node.containsKey("pageHits")?node.get("pageHits"):node.get("pagehits") ;
            record.pageHits = (Integer)tempKey ;
            record.waiting = node.containsKey("waiting")?(Integer)node.get("waiting"):0;
            tempKey = node.containsKey("allocatedBytes")?node.get("allocatedBytes"):node.get("allocatedbytes") ;
            record.allocatedBytes = Long.valueOf(tempKey.toString());
            record.dabtabase = node.get("database").toString();
            tempKey = node.containsKey("authenticatedUser")?node.get("authenticatedUser"):node.get("authenticateduser") ;
            record.authenticatedUser = tempKey.toString();
            tempKey = node.containsKey("executingUser")?node.get("executingUser"):node.get("executinguser") ;
            record.executedUser = tempKey.toString();
            record.query = node.containsKey("query")?cleanQuery(node.get("query").toString()):"";
            o = node.get("runtime");
            if (o != null) {
                record.runtime = o.toString();
            }
            o = node.get("source");
            if (o != null) {
                String txt = o.toString();
                //txt = txt.replaceAll("\\t", " ");
                if( txt.startsWith("embedded-session")) {
                    record.client = "embedded-session" ;
                    record.authenticatedUser = txt.substring(16).trim() ;
                    record.executedUser = record.authenticatedUser;
                } else if( txt.startsWith("server-session")) {
                    record.client = "server-session" ;
                    record.server = txt.substring(14).trim().replace('\t', ' ') ;
                } else {
                    StringTokenizer tokens = new StringTokenizer(txt, "\\\t") ;
                    String session = tokens.nextToken() ;
                    if( session.equals("bolt-session")) {
                        tokens.nextToken() ;

                        String driverTxt = tokens.nextToken() ;
                        int index = driverTxt.indexOf("/") ;
                        if( index > 0 ) {
                            record.driver = driverTxt.substring(0,index) ;
                            record.driverVersion = driverTxt.substring(index+1) ;
                        }

                        record.client = getKeyValue(tokens.nextToken(), "client/", ':') ;
                        record.server = getKeyValue(tokens.nextToken(), "server/", ':') ;
                    } else {
                        System.out.println("Unknown Session : " + txt);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(node.toString());
            e.printStackTrace();
        }
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

    private String getKeyValue(String part, String key, char delimiter) {
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

    private void readAnnotationData(QueryRecord record, Map<String, Object> node) {
        String type = configuration.get("annotationRecordType") == null ? "flatten" : configuration.get("annotationRecordType").toString()  ;

        if( type.equals("flatten") ) {
            Iterator<String> fields = node.keySet().iterator();
            if (fields.hasNext()) {
                record.annotationData = new HashMap<>();
                while (fields.hasNext()) {
                    String field = fields.next();
                    if (field.startsWith("annotationData.") || field.startsWith("annotationdata.")) {

                        record.annotationData.put(field.replace("annotationdata.", "annotationData."), node.get(field).toString());
                    }
                }
            }
        } else if( type.equals("map") ) {
            Map<String, Object> n = (Map<String, Object>)node.get("annotationData") ;
            if( n == null ) {
                n = (Map<String, Object>)node.get("annotationdata") ;
            }
            if( n != null ) {
                Iterator<String> fields = n.keySet().iterator();
                if (fields.hasNext()) {
                    record.annotationData = new HashMap<>();
                    while (fields.hasNext()) {
                        String field = fields.next();
                        if( field.equals("source")) {
                            Map<String, Object> src = (Map<String, Object>)n.get(field) ;
                            String query = src.get("query").toString() ;
                            record.annotationData.put("annotationData.source.query", query);
                        } else {
                            record.annotationData.put("annotationData."+field, n.get(field).toString());
                        }
                    }
                }
            }
        }
    }

    private String cleanQuery(String query) {
        String modified = query.toLowerCase() ;
        if( modified.startsWith("cypher ")) {
            // We need to take out first 2 words.
//            int index = query.indexOf(' ', 7) ;
//            int tabIndex = query.indexOf('\t', 7) ;
//            if( tabIndex > 0 && tabIndex < index ) {
//                index = tabIndex ;
//            }
//            tabIndex = query.indexOf('\n', 7) ;
//            if( tabIndex > 0 && tabIndex < index ) {
//                index = tabIndex ;
//            }
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
}
