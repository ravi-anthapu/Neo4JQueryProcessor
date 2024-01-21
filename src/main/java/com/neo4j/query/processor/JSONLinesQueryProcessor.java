package com.neo4j.query.processor;

import com.neo4j.query.QueryRecord;
import com.neo4j.query.database.IStorageAdapter;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class JSONLinesQueryProcessor implements QueryProcessor {
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

            ObjectMapper mapper = new ObjectMapper() ;

            BufferedReader reader = new BufferedReader(new FileReader(file)) ;
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
                    System.out.println(file + " :: " + new Date() + " :: index : " + counter);
                    storageAdapter.commit();
                }
                counter++ ;

                QueryRecord record = readQueryLogEntry(data) ;
                if( record == null )
                    continue;
                if( record.isStartRecord ) {
                    storageAdapter.addQueryStart(record);
                } else {
                    storageAdapter.addQueryEnd(record);
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

            // This is the JSON format of query log downloaded by customers.
            timestamp = node.get("time").toString().substring(0, 19).replace('T', ' ');

            Timestamp sts = Timestamp.valueOf(timestamp);

            record = new QueryRecord();
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

            record.dbQueryId = Long.valueOf(node.get("id").toString());
            Object o = node.get("transactionId") ;
            if( o != null ) {
                record.dbTransactionId = Long.valueOf(o.toString());
            }

            record.planning = node.containsKey("planning")?(Integer)node.get("planning"):0;
            record.elapsedTimeMs = (Integer) node.get("elapsedTimeMs");
            if( record.elapsedTimeMs == 0 ) {
                record.elapsedTimeMs = 1 ;
            }
            record.pageFaults = (Integer)node.get("pageFaults") ;
            record.pageHits = (Integer)node.get("pageHits") ;
            record.waiting = node.containsKey("waiting")?(Integer)node.get("waiting"):0;
            record.allocatedBytes = Long.valueOf(node.get("allocatedBytes").toString());
            record.dabtabase = node.get("database").toString();
//            record.dbId = node.get("dbid").toString();
            record.authenticatedUser = node.get("authenticatedUser").toString();
            record.executedUser = node.get("executingUser").toString();
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
        Iterator<String> fields = node.keySet().iterator(); ;
        if( fields.hasNext() ) {
            record.annotationData = new HashMap<>() ;
            while (fields.hasNext()) {
                String field = fields.next() ;
                if( field.startsWith("annotationData.")) {
                    record.annotationData.put(field, node.get(field)) ;
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
