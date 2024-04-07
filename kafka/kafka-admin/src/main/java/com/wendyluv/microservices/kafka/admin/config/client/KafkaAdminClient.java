package com.wendyluv.microservices.kafka.admin.config.client;

import com.wendyluv.microservices.config.KafkaConfigData;
import com.wendyluv.microservices.config.RetryConfigData;
import com.wendyluv.microservices.kafka.admin.config.exception.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;


@Component
public class KafkaAdminClient {
    private  static  final  Logger  log = LoggerFactory.getLogger(KafkaAdminClient.class);
    private  final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private  final AdminClient adminClient;
    private  final RetryTemplate retryTemplate;

    public KafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData, AdminClient adminClient, RetryTemplate retryTemplate) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;

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

    private Collection <TopicListing>  doGetTopics(RetryContext retryContext){
        log.info("Reading Kafka topic {}, attempt {}", kafkaConfigData.getTopicNamesToCreate().toArray(), retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        if(topics != null){
            topics.forEach(topic -> log.debug("Topic with name {}", topic.name()));
        }
        return topics;

    }


}
