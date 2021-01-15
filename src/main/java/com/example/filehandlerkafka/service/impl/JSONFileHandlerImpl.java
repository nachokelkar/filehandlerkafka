package com.example.filehandlerkafka.service.impl;

import com.example.filehandlerkafka.entity.Employee;
import com.example.filehandlerkafka.service.FileHandler;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;

@EnableKafka
@Service
public class JSONFileHandlerImpl implements FileHandler {
    @Autowired
    KafkaTemplate<String, Employee> kafkaTemplate;

    @Override
    public void read() {
        JSONParser jsonParser = new JSONParser();
        try (FileReader reader = new FileReader("/Users/pranavkelkar/Downloads/employee.json"))
        {
            //Read JSON file
            Object obj = jsonParser.parse(reader);
            JSONArray employeeList = (JSONArray) obj;
            //Iterate over employee array
            employeeList.forEach( emp -> {
                try {
                    parseEmployeeObject( (JSONObject) emp );
                } catch (java.text.ParseException e) {
                    System.out.println(e);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void parseEmployeeObject(JSONObject employee) throws java.text.ParseException {
        //Create employee object within list
        Employee emp = new Employee();

        //Get employee first name
        String firstName = (String) employee.get("firstName");

        emp.setFirstName(firstName);
        //Get employee last name
        String lastName = (String) employee.get("lastName");
//        System.out.println(lastName);
        emp.setLastName(lastName);

        String  dateOfBirth = (String) employee.get("dateOfBirth");
//        System.out.println(dateOfBirth);
        emp.setDateOfBirth(new SimpleDateFormat("dd/MM/yyyy").parse(dateOfBirth));

        long experience = (long) employee.get("experience");
//        System.out.println(experience);
        emp.setExperience(experience);

        kafkaTemplate.send("JSON", emp);
    }

    @Override
    @KafkaListener(topics = "JSON", groupId = "${spring.kafka.consumer.group-id}")
    public void write(Employee employee) {
        try {
            FileWriter file = new FileWriter("/Users/pranavkelkar/Downloads/POSTGREStoJSON.json");

            JSONObject obj = new JSONObject();

            obj.put("firstName",employee.getFirstName());
            obj.put("lastName",employee.getLastName());
            obj.put("dateOfBirth",employee.getDateOfBirth());
            obj.put("experience",employee.getExperience());

            file.write(obj.toJSONString());

            file.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

}
