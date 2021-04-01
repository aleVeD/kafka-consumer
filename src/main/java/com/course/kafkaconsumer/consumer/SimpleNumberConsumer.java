package com.course.kafkaconsumer.consumer;

import com.course.kafkaconsumer.entity.SimpleNumber;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class SimpleNumberConsumer {

    private static final Logger log = LoggerFactory.getLogger(SimpleNumberConsumer.class);
    private ObjectMapper mapper = new ObjectMapper();
    @KafkaListener(topics = "t_simple_number")
    public void cunsume(String message) throws JsonProcessingException {
        SimpleNumber simpleNumber = mapper.readValue(message, SimpleNumber.class);
        if(simpleNumber.getNumber() %2 != 0){
            throw new IllegalArgumentException("Odd number");
        }

        log.info("valid number: {}", simpleNumber);
    }
}
