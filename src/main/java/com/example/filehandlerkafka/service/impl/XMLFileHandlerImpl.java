package com.example.filehandlerkafka.service.impl;

import com.example.filehandlerkafka.entity.Employee;
import com.example.filehandlerkafka.service.FileHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;

@Service
public class XMLFileHandlerImpl implements FileHandler {
    @Autowired
    KafkaTemplate<String, Employee> kafkaTemplate;

    public static Iterable<Node> iterable(final NodeList nodeList) {
        return () -> new Iterator<Node>() {

            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < nodeList.getLength();
            }

            @Override
            public Node next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                return nodeList.item(index++);
            }
        };
    }

    @Override
    public void read() {
        try {
            File xmlFile = new File("/Users/pranavkelkar/Downloads/employee.xml");

            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(xmlFile);
            doc.getDocumentElement().normalize();
            NodeList nodeList = doc.getElementsByTagName("employee");


            for(Node node : iterable(nodeList)){
                Employee employeeXML = new Employee();

                if (node.getNodeType() == Node.ELEMENT_NODE){
                    Element e=(Element) node;

                    employeeXML.setFirstName(e.getElementsByTagName("firstName").item(0).getTextContent());
                    employeeXML.setLastName(e.getElementsByTagName("lastName").item(0).getTextContent());
                    employeeXML.setDateOfBirth(new SimpleDateFormat("dd/MM/yyyy").parse(e.getElementsByTagName("dateOfBirth").item(0).getTextContent()));
                    employeeXML.setExperience(Double.parseDouble(e.getElementsByTagName("experience").item(0).getTextContent()));

                    kafkaTemplate.send("XML", employeeXML);
                }

            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    @KafkaListener(topics = "XML", groupId = "${spring.kafka.consumer.group-id}")
    public void write(Employee employee) {

        try {
            File xmlFile = new File("/Users/pranavkelkar/Downloads/employee.xml");
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            if(!xmlFile.exists()){
                xmlFile.createNewFile();
                Document doc = db.parse(xmlFile);
                Element root = doc.createElement("employees");
                doc.appendChild(root);
            }

            Document doc = db.parse(xmlFile);
            Element rootElement = doc.getDocumentElement();

            Element employeeElement= doc.createElement("employee");
            rootElement.appendChild(employeeElement);
            Element firstName=doc.createElement("firstName");
            firstName.appendChild(doc.createTextNode(employee.getFirstName()+" "));
            employeeElement.appendChild(firstName);
            Element lastName=doc.createElement("lastName");
            lastName.appendChild(doc.createTextNode(employee.getLastName()+" "));
            employeeElement.appendChild(lastName);
            Element dateOfBirth=doc.createElement("dateOfBirth");
            dateOfBirth.appendChild(doc.createTextNode(employee.getDateOfBirth()+" "));
            employeeElement.appendChild(dateOfBirth);
            Element experience=doc.createElement("experience");
            experience.appendChild(doc.createTextNode(employee.getExperience()+" "));
            employeeElement.appendChild(experience);


            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource domSource = new DOMSource(doc);
            StreamResult streamResult = new StreamResult(new File("/Users/pranavkelkar/Downloads/MONGOtoXML.xml"));
            transformer.transform(domSource, streamResult);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

}

