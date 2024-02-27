package com.neo4j.query.processor;

import com.neo4j.query.QueryRecord;
import com.neo4j.query.database.IStorageAdapter;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.MappingJsonFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class AuraJSONQueryProcessor implements QueryProcessor {
    private IStorageAdapter storageAdapter ;

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
    private Map<String, Object> configuration ;

    @Override
    public void initialize(Map<String, Object> configuration, IStorageAdapter storageAdapter) {
        this.configuration = configuration ;
        this.storageAdapter = storageAdapter ;
    }

    @Override
    public void processFile(String fileName, InputStream is) {
        try {
            System.out.println("Processing File : " + fileName);
            JsonFactory f = new MappingJsonFactory();
            InputStream fis = is;
            JsonParser jp = f.createJsonParser(fis);
            JsonToken current;
            int counter = 0 ;

            while( ( current = jp.nextToken() ) != JsonToken.END_OBJECT ) {
                if (current == null) {
                    break;
                }
                if( current == JsonToken.START_OBJECT ) {
//                    if(counter % 1000 == 0 ) {
//                        System.out.println(file + " :: " + new Date() + " :: index : " + counter);
//                    }
                    if(counter % 50000 == 0 ) {
                        System.out.println(fileName + " :: " + new Date() + " :: index : " + counter);
                        storageAdapter.commit();
                    }
                    counter++ ;
                    JsonNode node = jp.readValueAsTree();
                    QueryRecord record = readQueryLogEntry(node) ;
                    if( record == null )
                        continue;
                    if( record.isStartRecord ) {
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

    private QueryRecord readQueryLogEntry(JsonNode node) {
        QueryRecord record = null ;

        try {

            String timestamp = null ;
            if( node.has("timestamp")) {
                // This is the Aura JSON log format that is exported by support.
                timestamp = node.get("timestamp").asText().substring(0, 19).replace('T', ' ');
            } else if( node.has("time")) {
                // This is the JSON format of query log downloaded by customers.
                timestamp = node.get("time").asText().substring(0, 19).replace('T', ' ');
            }
            Timestamp sts = Timestamp.valueOf(timestamp);

            JsonNode payload = node;
            if( payload.has("jsonPayload")) {
                payload = node.get("jsonPayload");
            }
            if (payload != null ) {
                record = new QueryRecord();
                record.timeStamp = sts;

                readAnnotationData(record, payload) ;

                String type = payload.get("type").asText();
                if( type != null && type.trim().equals("transaction")) {
                    return null ;
                }
                String event = payload.get("event").asText();
                if( event.equalsIgnoreCase("fail")) {
                    record.failed = 1 ;
                    record.stackTrace = payload.has("stacktrace")?payload.get("stacktrace").asText():null ;
                }

                if (event.equalsIgnoreCase("start")) {
                    record.isStartRecord = true;
                } else {
                    record.isStartRecord = false;
                }

                record.dbQueryId = payload.get("id").asLong();
                JsonNode o = payload.get("transactionId") ;
                if( o != null ) {
                    record.dbTransactionId = o.asLong();
                }

                record.planning = payload.has("planning")?payload.get("planning").asLong():0;
                record.elapsedTimeMs = payload.get("elapsedTimeMs").asLong();
                if( record.elapsedTimeMs == 0 ) {
                    record.elapsedTimeMs = 1 ;
                }
                record.pageFaults = payload.get("pageFaults").asLong();
                record.pageHits = payload.get("pageHits").asLong();
                record.waiting = payload.has("waiting")?payload.get("waiting").asLong():0;
                record.allocatedBytes = payload.get("allocatedBytes").asLong();
                record.dabtabase = payload.get("database").asText();
                record.dbId = payload.get("dbid").asText();
                record.authenticatedUser = payload.get("authenticatedUser").asText();
                record.executedUser = payload.get("executingUser").asText();
                record.query = payload.has("query")?cleanQuery(payload.get("query").asText()):"";
                o = payload.get("runtime");
                if (o != null) {
                    record.runtime = o.asText();
                }
                o = payload.get("source");
                if (o != null) {
                    String txt = o.asText();
                    //txt = txt.replaceAll("\\t", " ");
                    if( txt.startsWith("embedded-session")) {
                        record.client = "embedded-session" ;
                        record.authenticatedUser = txt.substring(16).trim() ;
                        record.executedUser = record.authenticatedUser;
                    } else {
                        StringTokenizer tokens = new StringTokenizer(txt, "\\\t") ;
                        /***
                        int index = txt.indexOf("client/");
                        int endIndex = txt.indexOf("server/");
                        if (endIndex == -1 || index == -1) {
                            System.out.println("Txt : " + txt);
                        } else {
                            String client = txt.substring(index + 7, endIndex).trim();
                            index = client.indexOf(":");
                            if (index > 0) {
                                record.client = client.substring(0, index);
                            }
                            index = txt.indexOf(":", endIndex+7);
                            if( index > 0 ) {
                                record.server = txt.substring(endIndex+7, index);
                            }
                        }
                         ***********/
                        String session = tokens.nextToken() ;
                        if( session.equals("bolt-session")) {
                            tokens.nextToken() ;

                            String driverTxt = tokens.nextToken() ;
                            int index = driverTxt.indexOf("/") ;
                            if( index > 0 ) {
                                record.driver = driverTxt.substring(0,index) ;
                                record.driverVersion = driverTxt.substring(index+1) ;
                            }
//                            String clientTxt = tokens.nextToken() ;
//                            String serverTxt = tokens.nextToken() ;
                            record.client = getKeyValue(tokens.nextToken(), "client/", ':') ;
                            record.server = getKeyValue(tokens.nextToken(), "server/", ':') ;
                        } else {
                            System.out.println("Unknown Session : " + txt);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(node.asText());
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

    private void readAnnotationData(QueryRecord record, JsonNode node) {
        String type = configuration.get("annotationRecordType") == null ? "flatten" : configuration.get("annotationRecordType").toString()  ;

        if( type.equals("flatten") ) {
            Iterator<String> fields = node.getFieldNames();
            if (fields.hasNext()) {
                record.annotationData = new HashMap<>();
                while (fields.hasNext()) {
                    String field = fields.next();
                    if (field.startsWith("annotationData.")) {
                        record.annotationData.put(field, node.get(field).asText());
                    }
                }
            }
        } else if( type.equals("map") ) {
            JsonNode n = node.get("annotationData") ;
            if( n != null ) {
                Iterator<String> fields = n.getFieldNames();
                if (fields.hasNext()) {
                    record.annotationData = new HashMap<>();
                    while (fields.hasNext()) {
                        String field = fields.next();
                        if( field.equals("source")) {
                            JsonNode src = n.get(field) ;
                            String query = src.get("query").toString() ;
                            record.annotationData.put("annotationData.source.query", query);
                        } else {
                            record.annotationData.put("annotationData."+field, n.get(field).asText());
                        }
                    }
                }
            }
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
