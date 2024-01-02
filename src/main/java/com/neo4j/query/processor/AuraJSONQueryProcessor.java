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
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

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
    public void processFile(File file) {
        try {
            System.out.println("Processing File : " + file.getAbsolutePath());
            JsonFactory f = new MappingJsonFactory();
            FileInputStream fis = new FileInputStream(file);
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
                        System.out.println(file + " :: " + new Date() + " :: index : " + counter);
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
            String timestamp = node.get("timestamp").asText().substring(0, 19).replace('T', ' ');
            Timestamp sts = Timestamp.valueOf(timestamp);
            JsonNode payload = node;
            if (payload.has("jsonPayload")) {
                payload = node.get("jsonPayload");
                record = new QueryRecord();
                record.timeStamp = sts;

                String event = payload.get("event").asText();
                if( event.equalsIgnoreCase("fail")) {
                    return null ;
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

                record.planning = payload.get("planning").asLong();
                record.elapsedTimeMs = payload.get("elapsedTimeMs").asLong();
                if( record.elapsedTimeMs == 0 ) {
                    record.elapsedTimeMs = 1 ;
                }
                record.pageFaults = payload.get("pageFaults").asLong();
                record.pageHits = payload.get("pageHits").asLong();
                record.waiting = payload.get("waiting").asLong();
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
                    txt = txt.replaceAll("\\t", " ");
                    if( txt.startsWith("embedded-session")) {
                        record.client = txt.trim() ;
                    } else {
                        int index = txt.indexOf("client");
                        int endIndex = txt.indexOf("server");
                        if (endIndex == -1 || index == -1) {
                            System.out.println("Txt : " + txt);
                        } else {
                            String client = txt.substring(index + 7, endIndex).trim();
                            index = client.indexOf(":");
                            if (index > 0) {
                                record.client = client.substring(0, index);
                            }
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
