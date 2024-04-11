package com.wendyluv.microservices.twitter_to_kafka.service;

import com.wendyluv.microservices.config.TwitterToKafkaServiceConfigData;
import com.wendyluv.microservices.twitter_to_kafka.service.runner.StreamRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ComponentScan;

import java.util.Arrays;

@SpringBootApplication
@ComponentScan(basePackages = {"com.wendyluv.microservices" })
public class TwitterToKafkaApplication implements CommandLineRunner {

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfig;
    private static final Logger log = LoggerFactory.getLogger(TwitterToKafkaApplication.class);
    private final StreamRunner streamRunner;

    public TwitterToKafkaApplication(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfig, StreamRunner streamRunner) {
        this.twitterToKafkaServiceConfig = twitterToKafkaServiceConfig;
        this.streamRunner = streamRunner;
    }

    public static void main(String[] args){
        SpringApplication.run(TwitterToKafkaApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception{
        log.info("Application is starting");
        log.info(Arrays.toString(twitterToKafkaServiceConfig.getTwitterKeywords().toArray(new String[]{})));
        log.info(twitterToKafkaServiceConfig.getLoggingMessage());
        streamRunner.start();
    }


}
