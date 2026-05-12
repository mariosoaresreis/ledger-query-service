package com.marioreis.ledgerquery.api;

import com.marioreis.ledgerquery.config.KafkaConsumerConfig;
import com.marioreis.ledgerquery.persistence.ProjectionRepository;
import com.marioreis.ledgerquery.service.BalanceCacheService;
import com.marioreis.ledgerquery.service.KafkaLagService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(
        value = LedgerQueryController.class,
        // Exclude KafkaConsumerConfig so AdminClient is never created (avoids background threads)
        excludeFilters = @ComponentScan.Filter(
                type = FilterType.ASSIGNABLE_TYPE,
                classes = KafkaConsumerConfig.class
        )
)
class LedgerQueryControllerTest {

    @Autowired MockMvc mvc;

    @MockBean ProjectionRepository projectionRepository;
    @MockBean BalanceCacheService  balanceCacheService;
    @MockBean KafkaLagService      kafkaLagService;

    // Satisfies RedisConfig which is picked up by @WebMvcTest component scan
    @MockBean RedisConnectionFactory redisConnectionFactory;

    private static final UUID ACCOUNT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID OWNER_ID   = UUID.fromString("22222222-2222-2222-2222-222222222222");

    // ── Balance ───────────────────────────────────────────────────────────────

    @Test
    void getBalance_cacheHit_returns200WithBody() throws Exception {
        BalanceResponse cached = new BalanceResponse(
                ACCOUNT_ID, new BigDecimal("1500.00"), "USD",
                OffsetDateTime.parse("2024-11-01T00:00:00Z"));
        when(balanceCacheService.getBalance(ACCOUNT_ID)).thenReturn(cached);

        mvc.perform(get("/api/v1/accounts/{id}/balance", ACCOUNT_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accountId").value(ACCOUNT_ID.toString()))
                .andExpect(jsonPath("$.balance").value(1500.00))
                .andExpect(jsonPath("$.currency").value("USD"));

        verifyNoInteractions(projectionRepository);
    }

    @Test
    void getBalance_cacheMiss_loadsFromDbAndCaches() throws Exception {
        when(balanceCacheService.getBalance(ACCOUNT_ID)).thenReturn(null);

        Map<String, Object> row = Map.of(
                "balance", new BigDecimal("1234.56"),
                "currency", "BRL",
                "updated_at", OffsetDateTime.parse("2024-10-01T12:00:00Z"));
        when(projectionRepository.findBalance(ACCOUNT_ID)).thenReturn(Optional.of(row));

        mvc.perform(get("/api/v1/accounts/{id}/balance", ACCOUNT_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.balance").value(1234.56))
                .andExpect(jsonPath("$.currency").value("BRL"));

        verify(balanceCacheService).putBalance(eq(ACCOUNT_ID), any());
    }

    @Test
    void getBalance_accountNotFound_returns404() throws Exception {
        when(balanceCacheService.getBalance(ACCOUNT_ID)).thenReturn(null);
        when(projectionRepository.findBalance(ACCOUNT_ID)).thenReturn(Optional.empty());

        mvc.perform(get("/api/v1/accounts/{id}/balance", ACCOUNT_ID))
                .andExpect(status().isNotFound());
    }

    // ── Transactions ──────────────────────────────────────────────────────────

    @Test
    void getTransactions_defaultPaging_returnsPagedResult() throws Exception {
        UUID txId = UUID.randomUUID();
        Map<String, Object> row = new HashMap<>();
        row.put("id",          txId);
        row.put("account_id",  ACCOUNT_ID);
        row.put("amount",      new BigDecimal("100.00"));
        row.put("direction",   "CREDIT");
        row.put("counterpart", null);
        row.put("reference",   "salary");
        row.put("occurred_at", OffsetDateTime.parse("2024-11-01T09:00:00Z"));

        when(projectionRepository.findTransactions(eq(ACCOUNT_ID), isNull(), isNull(), isNull(), eq(0), eq(20)))
                .thenReturn(List.of(row));
        when(projectionRepository.countTransactions(eq(ACCOUNT_ID), isNull(), isNull(), isNull()))
                .thenReturn(1L);

        mvc.perform(get("/api/v1/accounts/{id}/transactions", ACCOUNT_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].direction").value("CREDIT"))
                .andExpect(jsonPath("$.content[0].amount").value(100.00))
                .andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.totalPages").value(1))
                .andExpect(jsonPath("$.page").value(0));
    }

    @Test
    void getTransactions_withDateRangeAndDirection_passesFiltersThrough() throws Exception {
        when(projectionRepository.findTransactions(any(), any(), any(), eq("DEBIT"), eq(0), eq(10)))
                .thenReturn(Collections.emptyList());
        when(projectionRepository.countTransactions(any(), any(), any(), eq("DEBIT")))
                .thenReturn(0L);

        mvc.perform(get("/api/v1/accounts/{id}/transactions", ACCOUNT_ID)
                .param("from",      "2024-01-01")
                .param("to",        "2024-12-31")
                .param("direction", "DEBIT")
                .param("size",      "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(0));

        verify(projectionRepository).findTransactions(
                eq(ACCOUNT_ID), notNull(), notNull(), eq("DEBIT"), eq(0), eq(10));
    }

    // ── Statement ─────────────────────────────────────────────────────────────

    @Test
    void getStatement_computesOpeningBalance() throws Exception {
        Map<String, Object> txRow = new HashMap<>();
        txRow.put("id",          UUID.randomUUID());
        txRow.put("account_id",  ACCOUNT_ID);
        txRow.put("amount",      new BigDecimal("200.00"));
        txRow.put("direction",   "CREDIT");
        txRow.put("counterpart", null);
        txRow.put("reference",   null);
        txRow.put("occurred_at", OffsetDateTime.parse("2024-11-15T08:00:00Z"));

        Map<String, Object> balRow = Map.of(
                "balance",    new BigDecimal("1000.00"),
                "currency",   "USD",
                "updated_at", OffsetDateTime.parse("2024-11-01T12:00:00Z"));

        when(projectionRepository.findTransactions(
                eq(ACCOUNT_ID), any(), any(), isNull(), eq(0), eq(Integer.MAX_VALUE)))
                .thenReturn(List.of(txRow));
        when(projectionRepository.findBalance(ACCOUNT_ID)).thenReturn(Optional.of(balRow));

        mvc.perform(get("/api/v1/accounts/{id}/statement", ACCOUNT_ID)
                .param("month", "2024-11"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.month").value("2024-11"))
                .andExpect(jsonPath("$.closingBalance").value(1000.00))
                // openingBalance = 1000 − 200 (one CREDIT) = 800
                .andExpect(jsonPath("$.openingBalance").value(800.00))
                .andExpect(jsonPath("$.entries").isArray())
                .andExpect(jsonPath("$.entries.length()").value(1));
    }

    @Test
    void getStatement_accountNotFound_returns404() throws Exception {
        when(projectionRepository.findTransactions(any(), any(), any(), isNull(), eq(0), eq(Integer.MAX_VALUE)))
                .thenReturn(Collections.emptyList());
        when(projectionRepository.findBalance(ACCOUNT_ID)).thenReturn(Optional.empty());

        mvc.perform(get("/api/v1/accounts/{id}/statement", ACCOUNT_ID)
                .param("month", "2024-11"))
                .andExpect(status().isNotFound());
    }

    // ── Events ─────────────────────────────────────────────────────────────────

    @Test
    void getEvents_returnsAuditLog() throws Exception {
        when(projectionRepository.findEventLog(ACCOUNT_ID)).thenReturn(Collections.emptyList());

        mvc.perform(get("/api/v1/accounts/{id}/events", ACCOUNT_ID))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());
    }

    // ── Accounts by owner ─────────────────────────────────────────────────────

    @Test
    void getAccountsByOwner_returnsAccountList() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("account_id", ACCOUNT_ID);
        row.put("owner_id",   OWNER_ID);
        row.put("status",     "ACTIVE");
        row.put("currency",   "USD");
        row.put("created_at", OffsetDateTime.parse("2024-01-01T00:00:00Z"));

        when(projectionRepository.findAccountsByOwner(OWNER_ID)).thenReturn(List.of(row));

        mvc.perform(get("/api/v1/accounts").param("ownerId", OWNER_ID.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].accountId").value(ACCOUNT_ID.toString()))
                .andExpect(jsonPath("$[0].status").value("ACTIVE"))
                .andExpect(jsonPath("$[0].currency").value("USD"));
    }

    // ── Replay (admin) ────────────────────────────────────────────────────────

    @Test
    void replay_recomputesBalanceAndEvictsCache_returns202() throws Exception {
        mvc.perform(post("/api/v1/admin/replay/{id}", ACCOUNT_ID))
                .andExpect(status().isAccepted());

        verify(projectionRepository).replayBalance(ACCOUNT_ID);
        verify(balanceCacheService).evict(ACCOUNT_ID);
    }

    // ── Kafka lag ─────────────────────────────────────────────────────────────

    @Test
    void getKafkaLag_returnsLagMap() throws Exception {
        when(kafkaLagService.getLag()).thenReturn(
                Map.of("totalLag", 0L, "groupId", "test-group", "topic", "test.events"));

        mvc.perform(get("/api/v1/health/lag"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalLag").value(0));
    }

    // ── Exception handler ─────────────────────────────────────────────────────

    @Test
    void invalidUuidPathVariable_returns400() throws Exception {
        mvc.perform(get("/api/v1/accounts/{id}/balance", "not-a-uuid"))
                .andExpect(status().isBadRequest());
    }
}

