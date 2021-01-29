package com.armory.logfiles;

import com.armory.logfiles.service.FileService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LogFilesOutputApplication implements CommandLineRunner {

    private static Log log = LogFactory.getLog("LogFilesOutputApplication");

    @Autowired
    private FileService fileService;

    public static void main(String[] args) {
        SpringApplication.run(LogFilesOutputApplication.class, args);
    }

    /**
     * Method to execute Spring boot application and call FileService process
     * @param args
     * @throws Exception
     */
    @Override
    public void run(String... args) throws Exception {
        log.info("Entered to sync and print logs");
        fileService.syncLogsServers();
        log.info("Sync and print logs finished");
    }
}
