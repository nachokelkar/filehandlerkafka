package com.example.filehandlerkafka.service;

import org.springframework.beans.factory.annotation.Autowired;

public class FileThread extends Thread {
    @Autowired
    FileHandler threadFileHandler;

    public FileThread(FileHandler fileHandler){
        this.threadFileHandler = fileHandler;
    }


    @Override
    public void run(){
        this.threadFileHandler.read();
    }
}
