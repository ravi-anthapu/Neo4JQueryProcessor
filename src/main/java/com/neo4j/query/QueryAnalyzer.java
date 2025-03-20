package com.neo4j.query;

import com.neo4j.query.database.IStorageAdapter;
import com.neo4j.query.database.SQLiteAdapter;
import com.neo4j.query.processor.AuraJSONQueryProcessor;
import com.neo4j.query.processor.FormatterQueryProcessor;
import com.neo4j.query.processor.JSONLinesQueryProcessor;
import com.neo4j.query.processor.QueryProcessor;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class QueryAnalyzer {

    public void process(String config) {
        Yaml yaml = new Yaml();
        try {

            InputStream inputStream = new FileInputStream(config);
            Map<String, Object> configuration = yaml.load(inputStream);
            QueryProcessor processor = null ;
            IStorageAdapter storageAdapter = null;

            Object o = configuration.get("fileType") ;
            String fileType = null ;
            if( o != null ) {
                fileType = o.toString() ;
            }
            String processorType = configuration.get("logType").toString() ;
            if( processorType == null || processorType.equals("formatted")) {
                processor = new FormatterQueryProcessor() ;
            } else if (processorType.equals("aura")) {
                processor = new AuraJSONQueryProcessor() ;
            } else if (processorType.equals("json_lines")) {
                processor = new JSONLinesQueryProcessor() ;
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
            for (File f : directoryListing) {
                if (pattern != null) {
                    String name = f.getName();
                    Matcher matcher = pattern.matcher(name);
                    if (!matcher.find()) {
                        System.out.println("Ignoring file as does not match the filter : " + name);
                        continue;
                    }
                }

                try {
                    if (fileType == null || fileType.equals("file")) {
                        processor.processFile(f.getAbsolutePath(), new FileInputStream(f), f.getName());
                    } else if (fileType.equals("tgz")) {
                        FileInputStream fis = new FileInputStream(f);
                        TarArchiveInputStream tarInput =
                                new TarArchiveInputStream(
                                        new GzipCompressorInputStream(fis));
                        TarArchiveEntry currentEntry = tarInput.getNextTarEntry();
                        while (currentEntry != null) {
                            processor.processFile(f.getAbsolutePath(), tarInput, f.getName());
                            currentEntry = tarInput.getNextTarEntry();
                        }
                    } else if (fileType.equals("gzip")) {
                        FileInputStream fis = new FileInputStream(f);
                        GzipCompressorInputStream gfis = new GzipCompressorInputStream(fis) ;
                        processor.processFile(f.getAbsolutePath(), gfis, f.getName());
                    } else if (fileType.equals("zip")) {
                        Pattern zipFilePattern = null ;
                        if( configuration.containsKey("zipFileNameFilter")) {
                            zipFilePattern = Pattern.compile(configuration.get("zipFileNameFilter").toString()) ;
                        }

                        FileInputStream fis = new FileInputStream(f);
                        ZipInputStream zis = new ZipInputStream(fis) ;
                        ZipEntry currentEntry = null ;
                        String curEntryName = null ;
                        while ( (currentEntry = zis.getNextEntry()) != null) {
                            if( zipFilePattern != null ) {
                                curEntryName = currentEntry.getName() ;
                                Matcher matcher = zipFilePattern.matcher(curEntryName) ;
                                if(!matcher.find()) {
                                    System.out.println("Ignoring file as does not match the filter : " + curEntryName);
                                    continue;
                                }
                            }

                            if( curEntryName.endsWith(".gz")) {
                                processor.processFile(f.getAbsolutePath(), new GzipCompressorInputStream(new CustomInputStream(zis)), f.getName());
                            } else {
                                processor.processFile(f.getAbsolutePath(), new CustomInputStream(zis), f.getName());
                            }
//                            zis.closeEntry();
                        }
                    }
                }catch (Exception ex) {
                    ex.printStackTrace();
                }
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
