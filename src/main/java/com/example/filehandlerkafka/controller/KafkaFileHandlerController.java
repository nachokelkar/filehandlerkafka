package com.example.filehandlerkafka.controller;

import com.example.filehandlerkafka.service.FileThread;
import com.example.filehandlerkafka.service.impl.CSVFileHandlerImpl;
import com.example.filehandlerkafka.service.impl.JSONFileHandlerImpl;
import com.example.filehandlerkafka.service.impl.XMLFileHandlerImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/kafkaFileHandler")
public class KafkaFileHandlerController {
    @Autowired
    CSVFileHandlerImpl csvRedisService;
    @Autowired
    JSONFileHandlerImpl jsonPostgresService;
    @Autowired
    XMLFileHandlerImpl xmlMongoService;

    @GetMapping("/rw")
    public String readWrite(){
        FileThread csvTh = new FileThread(csvRedisService);
        FileThread jsonTh = new FileThread(jsonPostgresService);
        FileThread xmlTh = new FileThread(xmlMongoService);

        csvTh.start();
        jsonTh.start();
        xmlTh.start();

        try{
            csvTh.join();
            jsonTh.join();
            xmlTh.join();
        }
        catch (Exception e){
            e.printStackTrace();
        }

        return "Read-Write API called";
    }

}
