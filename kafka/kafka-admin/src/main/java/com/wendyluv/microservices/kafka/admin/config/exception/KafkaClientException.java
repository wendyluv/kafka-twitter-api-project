package com.wendyluv.microservices.kafka.admin.config.exception;

public class KafkaClientException extends RuntimeException{
    public KafkaClientException() {
    }
    public KafkaClientException(String message){
        super(message);
    }
    public KafkaClientException(String message,Throwable cause){
        super(message, cause);
    }



}
