package com.marioreis.ledgerquery.service;

import com.marioreis.ledgerquery.api.BalanceResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class BalanceCacheServiceTest {

    @Mock
    RedisTemplate<String, String> redisTemplate;

    @Mock
    ValueOperations<String, String> valueOps;

    BalanceCacheService service;

    private static final UUID ACCOUNT_ID =
            UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    @BeforeEach
    void setUp() {
        // lenient: evict() tests don't go through opsForValue()
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOps);
        service = new BalanceCacheService(redisTemplate);
    }

    // ── getBalance ────────────────────────────────────────────────────────────

    @Test
    void getBalance_cacheHit_returnsDeserializedBalance() {
        when(valueOps.get("balance:" + ACCOUNT_ID))
                .thenReturn("1500.00|USD|2024-11-01T00:00:00Z");

        BalanceResponse result = service.getBalance(ACCOUNT_ID);

        assertThat(result).isNotNull();
        assertThat(result.accountId()).isEqualTo(ACCOUNT_ID);
        assertThat(result.balance()).isEqualByComparingTo("1500.00");
        assertThat(result.currency()).isEqualTo("USD");
        assertThat(result.asOf()).isEqualTo(OffsetDateTime.parse("2024-11-01T00:00:00Z"));
    }

    @Test
    void getBalance_cacheMiss_returnsNull() {
        when(valueOps.get("balance:" + ACCOUNT_ID)).thenReturn(null);

        assertThat(service.getBalance(ACCOUNT_ID)).isNull();
    }

    @Test
    void getBalance_redisThrows_returnsNullGracefully() {
        when(valueOps.get(anyString()))
                .thenThrow(new RuntimeException("Redis connection refused"));

        assertThat(service.getBalance(ACCOUNT_ID)).isNull();
    }

    @Test
    void getBalance_malformedCacheValue_returnsNull() {
        when(valueOps.get("balance:" + ACCOUNT_ID))
                .thenReturn("not|valid"); // only 2 parts, parse will fail

        assertThat(service.getBalance(ACCOUNT_ID)).isNull();
    }

    // ── putBalance ────────────────────────────────────────────────────────────

    @Test
    void putBalance_storesKeyValueWithTtl() {
        OffsetDateTime asOf = OffsetDateTime.parse("2024-11-01T10:00:00Z");
        BalanceResponse response = new BalanceResponse(ACCOUNT_ID, new BigDecimal("200.00"), "EUR", asOf);

        service.putBalance(ACCOUNT_ID, response);

        verify(valueOps).set(
                eq("balance:" + ACCOUNT_ID),
                eq("200.00|EUR|2024-11-01T10:00Z"),   // OffsetDateTime.toString() omits trailing :00 seconds
                eq(Duration.ofMinutes(10))
        );
    }

    @Test
    void putBalance_redisThrows_doesNotPropagate() {
        OffsetDateTime asOf = OffsetDateTime.parse("2024-11-01T10:00:00Z");
        BalanceResponse response = new BalanceResponse(ACCOUNT_ID, new BigDecimal("100.00"), "USD", asOf);
        doThrow(new RuntimeException("Redis down")).when(valueOps).set(anyString(), anyString(), any(Duration.class));

        assertThatCode(() -> service.putBalance(ACCOUNT_ID, response)).doesNotThrowAnyException();
    }

    // ── evict ─────────────────────────────────────────────────────────────────

    @Test
    void evict_deletesCorrectKey() {
        service.evict(ACCOUNT_ID);

        verify(redisTemplate).delete("balance:" + ACCOUNT_ID);
    }

    @Test
    void evict_redisThrows_doesNotPropagate() {
        doThrow(new RuntimeException("Redis down")).when(redisTemplate).delete(anyString());

        assertThatCode(() -> service.evict(ACCOUNT_ID)).doesNotThrowAnyException();
    }
}




