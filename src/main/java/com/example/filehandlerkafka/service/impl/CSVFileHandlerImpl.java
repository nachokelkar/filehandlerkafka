package com.example.filehandlerkafka.service.impl;

import com.example.filehandlerkafka.entity.Employee;
import com.example.filehandlerkafka.service.FileHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;

@EnableKafka
@Service
public class CSVFileHandlerImpl implements FileHandler {
    @Autowired
    KafkaTemplate<String, Employee> kafkaTemplate;

    @Override
    public void read() {
        String csvFile = "/Users/pranavkelkar/Downloads/employee.csv";
        BufferedReader in;
        String line = "";

        try{
            in = new BufferedReader(new FileReader(csvFile));
            while ((line = in.readLine()) != null){
                String[] employeeLine = line.split(",");
                Employee employee = new Employee();

                employee.setFirstName(employeeLine[0]);
                employee.setLastName(employeeLine[1]);
                Date dob = new SimpleDateFormat("MM/dd/yyyy").parse(employeeLine[2]);
                employee.setDateOfBirth(dob);
                employee.setExperience(Integer.parseInt(employeeLine[3]));

                kafkaTemplate.send("CSV", employee);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    @KafkaListener(topics = "CSV", groupId = "${spring.kafka.consumer.group-id}")
    public void write(Employee employee) {
        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/pranavkelkar/Downloads/REDIStoCSV.csv")));

            StringBuffer oneLine = new StringBuffer();
            oneLine.append(employee.getFirstName());
            oneLine.append(",");
            oneLine.append(employee.getLastName());
            oneLine.append(",");
            oneLine.append(employee.getDateOfBirth());
            oneLine.append(",");
            oneLine.append(employee.getExperience());
            writer.write(oneLine.toString());
            writer.newLine();
            writer.flush();
            writer.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

}
