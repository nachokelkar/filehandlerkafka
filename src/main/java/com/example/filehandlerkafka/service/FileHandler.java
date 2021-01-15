package com.example.filehandlerkafka.service;

import com.example.filehandlerkafka.entity.Employee;

public interface FileHandler{
    void read();
    void write(Employee employee);
}