package com.armory.logfiles.service;


import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Class to implement print logs functionality for each file
 */
@Service
public class FileService {

    private static Log log = LogFactory.getLog("FileService");

    /**
     * Executor to maintain the pool of threads
     */
    private static ExecutorService executorService;

    /**
     * Number of threads used to execute read file process
     */
    private static final int NO_OF_THREADS = 15;

    private static final String LOG_EXTENSION = "log";

    /**
     * Map is used to save the line from files
     */
    private SortedMap<LocalDateTime,String> mapLines;


    private String saveLocation;


    /**
     * Constructor of the class. Initialize the attributes.
     * @param saveLocation - Location of the logs from the server, it is configured in Application properties.
     */
    public FileService(@Value("${armory.filelocation}") String saveLocation) {
        mapLines = Collections.synchronizedSortedMap(new TreeMap<>());
        this.saveLocation = saveLocation;
        checkIfLocationExists();
    }

    /**
     * Init executor with initial number of threads.
     */
    private void initExecService() {
        try {
            executorService = Executors.newFixedThreadPool(NO_OF_THREADS);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Check the location of the logs, if it's not found then is created
     */
    private void checkIfLocationExists() {
        try {
            File file = new File(saveLocation);
            if(!file.exists()) file.mkdirs();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * Start process to print one output from all logs found of the servers
     */
    public void syncLogsServers() {
        initExecService();
        try {
            log.info("Number of logs founded: " + Files.list(Paths.get(saveLocation)).count());

            /**
             * Find the files in the directory. For each file found with the .log extension
             * start to read it and submit it to the thread pool of the executor
             */
            Files.list(Paths.get(saveLocation))
                    .filter(Files::isRegularFile)
                    .filter(file -> FilenameUtils.getExtension(file.getFileName().toString()).equals(LOG_EXTENSION))
                    .forEach(file -> executorService.submit(()-> readFile(file, Charset.forName("UTF-8"))));

            log.info("Proceed to terminate all thread");

            shutdownAndAwaitTermination(executorService,
                    150,
                    TimeUnit.MINUTES);

            executorService = null;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            try {
                executorService.shutdownNow();
                shutdownAndAwaitTermination(executorService, 5, TimeUnit.MINUTES);
                executorService = null;
            } catch (Exception e2) {
                log.error(e2.getMessage(), e2);
            }
        }
    }


    /**
     * Process to read each file from the servers.
     * @param file - File to be read it.
     * @param charset - Charset encode of the file.
     */
    private void readFile(Path file, Charset charset) {

        BufferedReader reader = null;
        try {
            log.info("Reading file " + file.getFileName().toString());
            reader = Files.newBufferedReader(file, charset);

            String line = null;
            while ((line = reader.readLine()) != null) {

                String[] splitLine =  line.split(",");
                LocalDateTime ldt = LocalDateTime.parse(splitLine[0], DateTimeFormatter.ISO_DATE_TIME);

                /**
                 * Creation of new map filtered to get all lines before the current date got from the current file line
                 */
                Map<LocalDateTime, String> filteredMap = mapLines.entrySet().stream()
                        .filter(entry -> entry.getKey().isBefore(ldt))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (v1,v2) ->{ throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));},
                                TreeMap::new));

                /**
                 * Printing each line from filter map, also we delete those objects to maintain memory.
                 */
                filteredMap.forEach((localDateTime, s) -> {
                    System.out.println(s);
                    mapLines.remove(localDateTime, s);
                });
                filteredMap.clear();

                mapLines.put(ldt, line);
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (MalformedInputException m) {
            readFile(file, Charset.forName("Windows-1252"));
        } catch (IOException e) {
            log.error(e, e);
        }
        catch (InterruptedException ie) {
            log.error(ie, ie);
        }
    }

    /**
     * Method used to shutdown all thread that are running.
     * @param pool - Executor service that has the threads running
     * @param shutdownTime - Maximum time to wait
     * @param unit - Time unit for the shutdown time
     */
    private void shutdownAndAwaitTermination(ExecutorService pool, int shutdownTime, TimeUnit unit) {
        if (pool != null) {
            log.info("Entered the shutdown method : total shutdown time is : " + shutdownTime + " time unit is : " + unit.toString());
            pool.shutdown();

            try {
                if (!pool.awaitTermination((long)shutdownTime, unit)) {
                    pool.shutdownNow();
                    if (!pool.awaitTermination(10L, TimeUnit.SECONDS)) {
                        log.info("Pool did not terminate");
                    }
                }
            } catch (InterruptedException var4) {
                pool.shutdownNow();
                Thread.currentThread().interrupt();
            } catch (Exception var5) {
                log.error("exception occured : " + var5.getMessage(), var5);
                log.error(var5.getMessage(), var5);
            }

            log.info("pool is shut down");
        }
    }


}
