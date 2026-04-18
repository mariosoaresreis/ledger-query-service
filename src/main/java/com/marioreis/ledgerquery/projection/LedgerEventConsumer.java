package com.marioreis.ledgerquery.projection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.marioreis.ledgerquery.domain.LedgerEventType;
import com.marioreis.ledgerquery.persistence.ProjectionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Kafka consumer that builds read-side projections from domain events.
 * Processes events from the command service and writes into:
 *   - account_summary      (account metadata)
 *   - account_balances     (current balance)
 *   - transaction_history  (per-transaction log)
 *   - event_log            (raw audit trail)
 */
@Component
public class LedgerEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(LedgerEventConsumer.class);

    private final ProjectionRepository projectionRepository;
    private final ObjectMapper objectMapper;

    public LedgerEventConsumer(ProjectionRepository projectionRepository, ObjectMapper objectMapper) {
        this.projectionRepository = projectionRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${ledger.kafka.topics.events}",
            groupId = "${ledger.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onEvent(@Payload String rawMessage,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset,
                        Acknowledgment acknowledgment) {
        try {
            LedgerEventMessage event = objectMapper.readValue(rawMessage, LedgerEventMessage.class);
            JsonNode payload = objectMapper.readTree(event.payload());

            // Store raw event in audit log first (idempotent by eventId)
            if (projectionRepository.eventLogExists(event.eventId())) {
                log.debug("Event {} already projected, skipping (partition={}, offset={})", event.eventId(), partition, offset);
                acknowledgment.acknowledge();
                return;
            }

            projectionRepository.insertEventLog(
                    event.eventId(), event.accountId(), event.eventType(), event.payload(), event.occurredAt());

            LedgerEventType type;
            try {
                type = LedgerEventType.valueOf(event.eventType());
            } catch (IllegalArgumentException e) {
                log.warn("Unknown event type '{}', skipping projection update", event.eventType());
                acknowledgment.acknowledge();
                return;
            }

            switch (type) {
                case ACCOUNT_CREATED -> projectAccountCreated(event, payload);
                case ACCOUNT_CREDITED -> projectCredit(event, payload);
                case ACCOUNT_DEBITED -> projectDebit(event, payload);
                case ACCOUNT_STATUS_CHANGED -> projectStatusChanged(event, payload);
                case TRANSFER_INITIATED -> projectTransferInitiated(event, payload);
                case TRANSFER_REVERSED -> projectTransferReversed(event, payload);
            }

            acknowledgment.acknowledge();
            log.debug("Projected event {} type={} account={}", event.eventId(), event.eventType(), event.accountId());

        } catch (Exception e) {
            log.error("Failed to project event from partition={} offset={}: {}", partition, offset, e.getMessage(), e);
            // Do not acknowledge – will be retried
        }
    }

    private void projectAccountCreated(LedgerEventMessage event, JsonNode payload) {
        UUID ownerId = UUID.fromString(payload.get("ownerId").asText());
        String currency = payload.get("currency").asText();
        String status = payload.get("status").asText();

        projectionRepository.upsertAccountSummary(
                event.accountId(), ownerId, status, currency, event.occurredAt());
        projectionRepository.upsertAccountBalance(
                event.accountId(), BigDecimal.ZERO, currency, event.eventId(), event.occurredAt());
    }

    private void projectCredit(LedgerEventMessage event, JsonNode payload) {
        BigDecimal amount = new BigDecimal(payload.get("amount").asText());
        String reference = payload.has("reference") ? payload.get("reference").asText() : null;
        UUID counterpart = extractCounterpart(reference);

        projectionRepository.adjustBalance(event.accountId(), amount, event.eventId(), event.occurredAt());
        projectionRepository.insertTransaction(
                UUID.randomUUID(), event.accountId(), amount, "CREDIT", counterpart, reference, event.occurredAt());
    }

    private void projectDebit(LedgerEventMessage event, JsonNode payload) {
        BigDecimal amount = new BigDecimal(payload.get("amount").asText());
        String reference = payload.has("reference") ? payload.get("reference").asText() : null;
        UUID counterpart = extractCounterpart(reference);

        projectionRepository.adjustBalance(event.accountId(), amount.negate(), event.eventId(), event.occurredAt());
        projectionRepository.insertTransaction(
                UUID.randomUUID(), event.accountId(), amount, "DEBIT", counterpart, reference, event.occurredAt());
    }

    private void projectStatusChanged(LedgerEventMessage event, JsonNode payload) {
        String toStatus = payload.get("toStatus").asText();
        projectionRepository.updateAccountStatus(event.accountId(), toStatus, event.occurredAt());
    }

    private void projectTransferInitiated(LedgerEventMessage event, JsonNode payload) {
        // Transfer saga: debit/credit events on individual accounts handle balance updates.
        // No balance change here — this event is for audit only (already stored in event_log).
        log.debug("TRANSFER_INITIATED recorded for transfer={}", event.accountId());
    }

    private void projectTransferReversed(LedgerEventMessage event, JsonNode payload) {
        // Compensating event: reverse the previously applied debit/credit
        if (payload.has("amount")) {
            BigDecimal amount = new BigDecimal(payload.get("amount").asText());
            String direction = payload.has("direction") ? payload.get("direction").asText() : "CREDIT";
            // Reverse: if original was DEBIT, reversal is CREDIT
            BigDecimal delta = "DEBIT".equals(direction) ? amount : amount.negate();
            projectionRepository.adjustBalance(event.accountId(), delta, event.eventId(), event.occurredAt());
            projectionRepository.insertTransaction(
                    UUID.randomUUID(), event.accountId(), amount,
                    "DEBIT".equals(direction) ? "CREDIT" : "DEBIT",
                    null, "reversal", event.occurredAt());
        }
    }

    private UUID extractCounterpart(String reference) {
        if (reference != null && reference.startsWith("transfer:")) {
            try {
                return UUID.fromString(reference.substring("transfer:".length()));
            } catch (IllegalArgumentException ignored) {
            }
        }
        return null;
    }
}

