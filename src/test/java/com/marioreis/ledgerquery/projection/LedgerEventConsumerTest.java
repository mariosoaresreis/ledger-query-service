package com.marioreis.ledgerquery.projection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.marioreis.ledgerquery.persistence.ProjectionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LedgerEventConsumerTest {

    @Mock ProjectionRepository repo;
    @Mock Acknowledgment ack;

    ObjectMapper objectMapper;
    LedgerEventConsumer consumer;

    private static final UUID EVENT_ID   = UUID.fromString("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee");
    private static final UUID ACCOUNT_ID = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID OWNER_ID   = UUID.fromString("11111111-1111-1111-1111-111111111111");

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        consumer = new LedgerEventConsumer(repo, objectMapper);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private String json(String eventType, String payload) throws Exception {
        LedgerEventMessage msg = new LedgerEventMessage(
                EVENT_ID, ACCOUNT_ID, eventType,
                OffsetDateTime.parse("2024-11-01T10:00:00Z"), payload);
        return objectMapper.writeValueAsString(msg);
    }

    // ── ACCOUNT_CREATED ───────────────────────────────────────────────────────

    @Test
    void accountCreated_upsertsAccountSummaryAndZeroBalance() throws Exception {
        when(repo.eventLogExists(EVENT_ID)).thenReturn(false);
        String payload = """
                {"ownerId":"%s","currency":"USD","status":"ACTIVE"}
                """.formatted(OWNER_ID);

        consumer.onEvent(json("ACCOUNT_CREATED", payload), 0, 0L, ack);

        verify(repo).insertEventLog(eq(EVENT_ID), eq(ACCOUNT_ID), eq("ACCOUNT_CREATED"), any(), any());
        verify(repo).upsertAccountSummary(eq(ACCOUNT_ID), eq(OWNER_ID), eq("ACTIVE"), eq("USD"), any());
        verify(repo).upsertAccountBalance(eq(ACCOUNT_ID), eq(BigDecimal.ZERO), eq("USD"), eq(EVENT_ID), any());
        verify(ack).acknowledge();
    }

    // ── ACCOUNT_CREDITED ────────────────────────────���─────────────────────────

    @Test
    void accountCredited_addsPositiveDeltaAndInsertsTransaction() throws Exception {
        when(repo.eventLogExists(EVENT_ID)).thenReturn(false);

        consumer.onEvent(json("ACCOUNT_CREDITED", """
                {"amount":"500.00","reference":"salary"}
                """), 0, 1L, ack);

        verify(repo).adjustBalance(eq(ACCOUNT_ID), eq(new BigDecimal("500.00")), eq(EVENT_ID), any());
        verify(repo).insertTransaction(
                any(), eq(ACCOUNT_ID), eq(new BigDecimal("500.00")),
                eq("CREDIT"), isNull(), eq("salary"), any());
        verify(ack).acknowledge();
    }

    @Test
    void accountCredited_transferReference_extractsCounterpart() throws Exception {
        when(repo.eventLogExists(EVENT_ID)).thenReturn(false);
        UUID counterpart = UUID.fromString("cccccccc-cccc-cccc-cccc-cccccccccccc");

        consumer.onEvent(json("ACCOUNT_CREDITED", """
                {"amount":"300.00","reference":"transfer:%s"}
                """.formatted(counterpart)), 0, 1L, ack);

        verify(repo).insertTransaction(
                any(), eq(ACCOUNT_ID), eq(new BigDecimal("300.00")),
                eq("CREDIT"), eq(counterpart), eq("transfer:" + counterpart), any());
    }

    // ── ACCOUNT_DEBITED ───────────────────────────────────────────────────────

    @Test
    void accountDebited_addsNegativeDeltaAndInsertsDebitTransaction() throws Exception {
        when(repo.eventLogExists(EVENT_ID)).thenReturn(false);

        consumer.onEvent(json("ACCOUNT_DEBITED", """
                {"amount":"100.00"}
                """), 0, 2L, ack);

        verify(repo).adjustBalance(eq(ACCOUNT_ID), eq(new BigDecimal("-100.00")), eq(EVENT_ID), any());
        verify(repo).insertTransaction(
                any(), eq(ACCOUNT_ID), eq(new BigDecimal("100.00")),
                eq("DEBIT"), isNull(), isNull(), any());
        verify(ack).acknowledge();
    }

    // ── ACCOUNT_STATUS_CHANGED ────────────────────────────────────────────────

    @Test
    void statusChanged_updatesAccountSummaryStatus() throws Exception {
        when(repo.eventLogExists(EVENT_ID)).thenReturn(false);

        consumer.onEvent(json("ACCOUNT_STATUS_CHANGED", """
                {"toStatus":"SUSPENDED"}
                """), 0, 3L, ack);

        verify(repo).updateAccountStatus(eq(ACCOUNT_ID), eq("SUSPENDED"), any());
        verify(ack).acknowledge();
    }

    // ── TRANSFER_INITIATED ────────────────────────────────────────────────────

    @Test
    void transferInitiated_onlyStoresAuditLogNoBalanceChange() throws Exception {
        when(repo.eventLogExists(EVENT_ID)).thenReturn(false);

        consumer.onEvent(json("TRANSFER_INITIATED", """
                {"fromAccount":"%s","toAccount":"%s","amount":"300.00"}
                """.formatted(ACCOUNT_ID, UUID.randomUUID())), 0, 4L, ack);

        verify(repo).insertEventLog(any(), any(), eq("TRANSFER_INITIATED"), any(), any());
        verify(repo, never()).adjustBalance(any(), any(), any(), any());
        verify(ack).acknowledge();
    }

    // ── TRANSFER_REVERSED ───────────────────────────────��─────────────────────

    @Test
    void transferReversed_originalDebit_appliesCreditReversal() throws Exception {
        when(repo.eventLogExists(EVENT_ID)).thenReturn(false);

        consumer.onEvent(json("TRANSFER_REVERSED", """
                {"amount":"150.00","direction":"DEBIT"}
                """), 0, 5L, ack);

        // Original was DEBIT → reversal delta is +150 (credit back)
        verify(repo).adjustBalance(eq(ACCOUNT_ID), eq(new BigDecimal("150.00")), eq(EVENT_ID), any());
        verify(repo).insertTransaction(
                any(), eq(ACCOUNT_ID), eq(new BigDecimal("150.00")),
                eq("CREDIT"), isNull(), eq("reversal"), any());
        verify(ack).acknowledge();
    }

    @Test
    void transferReversed_originalCredit_appliesDebitReversal() throws Exception {
        when(repo.eventLogExists(EVENT_ID)).thenReturn(false);

        consumer.onEvent(json("TRANSFER_REVERSED", """
                {"amount":"200.00","direction":"CREDIT"}
                """), 0, 6L, ack);

        // Original was CREDIT → reversal delta is -200
        verify(repo).adjustBalance(eq(ACCOUNT_ID), eq(new BigDecimal("-200.00")), eq(EVENT_ID), any());
        verify(repo).insertTransaction(
                any(), eq(ACCOUNT_ID), eq(new BigDecimal("200.00")),
                eq("DEBIT"), isNull(), eq("reversal"), any());
        verify(ack).acknowledge();
    }

    // ── Idempotency ───────────────────────────────────────────────────────────

    @Test
    void duplicateEvent_skipsProjectionButAcknowledges() throws Exception {
        when(repo.eventLogExists(EVENT_ID)).thenReturn(true);

        consumer.onEvent(json("ACCOUNT_CREATED", """
                {"ownerId":"%s","currency":"USD","status":"ACTIVE"}
                """.formatted(OWNER_ID)), 0, 0L, ack);

        verify(repo, never()).insertEventLog(any(), any(), any(), any(), any());
        verify(repo, never()).upsertAccountSummary(any(), any(), any(), any(), any());
        verify(ack).acknowledge();
    }

    // ── Unknown / malformed ───────────────────────────────────────────────────

    @Test
    void unknownEventType_storesAuditLogAndAcknowledgesWithoutUpdatingProjections() throws Exception {
        when(repo.eventLogExists(EVENT_ID)).thenReturn(false);

        consumer.onEvent(json("FUTURE_UNKNOWN_TYPE", "{}"), 0, 7L, ack);

        verify(repo).insertEventLog(any(), any(), eq("FUTURE_UNKNOWN_TYPE"), any(), any());
        verify(repo, never()).adjustBalance(any(), any(), any(), any());
        verify(ack).acknowledge();
    }

    @Test
    void malformedJson_doesNotAcknowledge_allowsRetry() throws Exception {
        consumer.onEvent("{ not valid json !!!", 0, 8L, ack);

        verify(ack, never()).acknowledge();
    }
}

