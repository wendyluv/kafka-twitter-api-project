package com.wendyluv.microservices.kafka.admin.config.client;

import com.wendyluv.microservices.config.KafkaConfigData;
import com.wendyluv.microservices.config.RetryConfigData;
import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;


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
}
