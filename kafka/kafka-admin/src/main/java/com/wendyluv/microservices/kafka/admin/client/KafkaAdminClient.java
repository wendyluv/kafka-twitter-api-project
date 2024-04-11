package com.wendyluv.microservices.kafka.admin.client;

import com.wendyluv.microservices.config.KafkaConfigData;
import com.wendyluv.microservices.config.RetryConfigData;
import com.wendyluv.microservices.kafka.admin.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import javax.net.ssl.SSLEngineResult;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


@Component
public class KafkaAdminClient {
    private  static  final  Logger  log = LoggerFactory.getLogger(KafkaAdminClient.class);
    private  final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private  final AdminClient adminClient;
    private  final RetryTemplate retryTemplate;
    private  final WebClient webClient;

    @Autowired
    public KafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData, AdminClient adminClient, RetryTemplate retryTemplate, WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;

    }

    public  void createTopics(){
        CreateTopicsResult createTopicsResult;
        try{
            createTopicsResult =  retryTemplate.execute(this::doCreateTopics);
        }
        catch(Throwable t){
            throw  new KafkaClientException("Reached max number of retries for creating kafka topics!",t );
        }
        checkTopicCreated();

    }

    public void checkTopicCreated(){
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        for (String topic: kafkaConfigData.getTopicNamesToCreate()){
            while (!isTopicCreated(topics, topic)){
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }

    }

    public  void checkSchemaRegistry(){
        int retryCount  =  1;
        Integer maxRetry = retryConfigData.getMaxAttempts();
        Integer multiplier = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while(!getSchemaRegistryStatus().is2xxSuccessful()){
            checkMaxRetry(retryCount,maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *= multiplier;
        }
    }

    private HttpStatus getSchemaRegistryStatus(){
        return HttpStatus.valueOf(webClient.method(HttpMethod.GET)
                .uri(kafkaConfigData.getGetSchemaRegistryUrl())
                .exchange()
                .map(ClientResponse::statusCode)
                .block()
                .value());
    }

    private void sleep(Long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        }catch (InterruptedException e){
            throw  new KafkaClientException("Error while sleeping, and waiting for new topics to be created.");
        }
    }

    private void checkMaxRetry(int retry, Integer maxRetry) {
        if(retry > maxRetry){
            throw new KafkaClientException("Reached max numnber of retries for reading kafka topics.");
        }

    }

    private boolean isTopicCreated(Collection<TopicListing> topics, String topicName) {
        if (topics == null){
            return  false;
        }
        return topics.stream().anyMatch(topic -> topic.name().equals(topicName));
    }



    private  CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        log.info("Creating {} topics, attempt {}", topicNames.size(), retryContext.getRetryCount());
        List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).collect(Collectors.toList());

        return adminClient.createTopics(kafkaTopics);
    }
    private Collection<TopicListing> getTopics(){
        Collection<TopicListing> topics;
        try {
            topics = retryTemplate.execute(this::doGetTopics);

        }catch (Throwable t){
            throw new KafkaClientException("Reached max number of retries for reading kafka topics.", t);
        }
        return topics;

    }

    private Collection <TopicListing>  doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        log.info("Reading Kafka topic {}, attempt {}", kafkaConfigData.getTopicNamesToCreate().toArray(), retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        if(topics != null){
            topics.forEach(topic -> log.debug("Topic with name {}", topic.name()));
        }
        return topics;

    }


}
