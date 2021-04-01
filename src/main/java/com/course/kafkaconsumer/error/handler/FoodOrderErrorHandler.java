package com.course.kafkaconsumer.error.handler;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

@Service(value = "myfoodErrorHandler")
public class FoodOrderErrorHandler implements ConsumerAwareListenerErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(FoodOrderErrorHandler.class);
    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
        log.warn("Food error message {} {}", message.getPayload(), exception);
        if(exception.getCause() instanceof RuntimeException){
            throw exception;
        }
        return null;
    }

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
        return null;
    }


}
