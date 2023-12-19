package com.neo4j.query;

import com.neo4j.query.database.IStorageAdapter;
import com.neo4j.query.processor.AuraJSONQueryProcessor;
import com.neo4j.query.processor.QueryProcessor;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

public class QueryAnalyzer {

    private IStorageAdapter storageAdapter ;

    public void process(String config) {
        Yaml yaml = new Yaml();
        InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream(config);
        Map<String, Object> configuration = yaml.load(inputStream);

        QueryProcessor processor = new AuraJSONQueryProcessor() ;
        processor.initialize(configuration);
        String queryDir = configuration.get("queryLocation").toString() ;

        try {
            File dir = new File(queryDir);
            File[] directoryListing = dir.listFiles();
            for( File f: directoryListing) {
                processor.processFile(f);
            }
        }catch (Exception e) {

        }

    }

    public static void main(String[] args) {
        QueryAnalyzer analyzer = new QueryAnalyzer() ;
        analyzer.process("config.yaml");
    }


}
