package com.neo4j.query;

import com.neo4j.query.database.IStorageAdapter;
import com.neo4j.query.database.SQLiteAdapter;
import com.neo4j.query.processor.AuraJSONQueryProcessor;
import com.neo4j.query.processor.FormatterQueryProcessor;
import com.neo4j.query.processor.QueryProcessor;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryAnalyzer {

    public void process(String config) {
        Yaml yaml = new Yaml();
        try {

            InputStream inputStream = new FileInputStream(config);
            Map<String, Object> configuration = yaml.load(inputStream);
            QueryProcessor processor = null ;
            IStorageAdapter storageAdapter = null;

            String processorType = configuration.get("logType").toString() ;
            if( processorType == null || processorType.equals("formatted")) {
                processor = new FormatterQueryProcessor() ;
            } else if (processorType.equals("aura")) {
                processor = new AuraJSONQueryProcessor() ;
            }

            String storageType = configuration.get("storeType").toString() ;
            if( storageType == null || storageType.equals("sqlite")) {
                storageAdapter = new SQLiteAdapter() ;
                storageAdapter.initialize(configuration);
                storageAdapter.setupStorage();

            }
            processor.initialize(configuration, storageAdapter);
            String queryDir = configuration.get("queryLocation").toString() ;

            Pattern pattern = null ;
            if( configuration.containsKey("fileFilter")) {
                pattern = Pattern.compile(configuration.get("fileFilter").toString()) ;
            }

            File dir = new File(queryDir);
            File[] directoryListing = dir.listFiles();
            for( File f: directoryListing) {
                if( pattern != null ) {
                    String name = f.getName();
                    Matcher matcher = pattern.matcher(name) ;
                    if(!matcher.find()) {
                        System.out.println("Ignoring file as does not match the filter : " + name);
                        continue;
                    }
                }

                processor.processFile(f);
            }
            processor.finishProcesing();
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        QueryAnalyzer analyzer = new QueryAnalyzer() ;
        analyzer.process(args[0]);
    }


}
