package com.course.kafkaconsumer.consumer;

import com.course.kafkaconsumer.entity.Image;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.xml.ws.http.HTTPException;
import java.net.HttpRetryException;

@Service
public class ImageConsumer {
    private static final Logger log = LoggerFactory.getLogger(ImageConsumer.class);

    ObjectMapper mapper = new ObjectMapper();
    @KafkaListener(topics = "t_image", containerFactory = "retryContainerFactory")
    private void consume(String message) throws Exception {
        Image image = mapper.readValue(message, Image.class);
        if(image.getType().equalsIgnoreCase( "svg")){
            throw new Exception("simulate failure");
        }

        log.info("procesing image: {}", image);
    }
}
