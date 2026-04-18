package com.marioreis.ledgerquery.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Reports Kafka consumer group lag for the projection consumer.
 */
@Service
public class KafkaLagService {

    private static final Logger log = LoggerFactory.getLogger(KafkaLagService.class);

    private final AdminClient adminClient;
    private final String groupId;
    private final String eventsTopic;

    public KafkaLagService(AdminClient adminClient,
                           @Value("${ledger.kafka.consumer.group-id}") String groupId,
                           @Value("${ledger.kafka.topics.events}") String eventsTopic) {
        this.adminClient = adminClient;
        this.groupId = groupId;
        this.eventsTopic = eventsTopic;
    }

    public Map<String, Object> getLag() {
        Map<String, Object> result = new HashMap<>();
        try {
            ListConsumerGroupOffsetsResult cgOffsets = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
                    cgOffsets.partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);

            // Filter to our topic
            Map<TopicPartition, OffsetSpec> latestRequest = new HashMap<>();
            committed.keySet().stream()
                    .filter(tp -> tp.topic().equals(eventsTopic))
                    .forEach(tp -> latestRequest.put(tp, OffsetSpec.latest()));

            if (latestRequest.isEmpty()) {
                result.put("lag", 0);
                result.put("partitions", Map.of());
                return result;
            }

            ListOffsetsResult latestOffsets = adminClient.listOffsets(latestRequest);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    latestOffsets.all().get(5, TimeUnit.SECONDS);

            Map<String, Long> perPartition = new HashMap<>();
            long totalLag = 0;
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> entry : endOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long end = entry.getValue().offset();
                long current = committed.getOrDefault(tp, new org.apache.kafka.clients.consumer.OffsetAndMetadata(0)).offset();
                long partitionLag = Math.max(0, end - current);
                perPartition.put("partition-" + tp.partition(), partitionLag);
                totalLag += partitionLag;
            }
            result.put("totalLag", totalLag);
            result.put("groupId", groupId);
            result.put("topic", eventsTopic);
            result.put("partitions", perPartition);
        } catch (Exception e) {
            log.warn("Could not retrieve Kafka lag: {}", e.getMessage());
            result.put("error", "Could not retrieve lag: " + e.getMessage());
        }
        return result;
    }
}

