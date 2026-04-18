package com.marioreis.ledgerquery.service;

import com.marioreis.ledgerquery.api.BalanceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Redis-backed balance cache.
 * Key: balance:{accountId}   Value: "<balance>|<currency>|<updatedAt ISO>"
 */
@Service
public class BalanceCacheService {

    private static final Logger log = LoggerFactory.getLogger(BalanceCacheService.class);
    private static final Duration TTL = Duration.ofMinutes(10);
    private static final String KEY_PREFIX = "balance:";

    private final RedisTemplate<String, String> redisTemplate;

    public BalanceCacheService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public BalanceResponse getBalance(UUID accountId) {
        try {
            String value = redisTemplate.opsForValue().get(KEY_PREFIX + accountId);
            if (value == null) return null;
            String[] parts = value.split("\\|", 3);
            return new BalanceResponse(
                    accountId,
                    new BigDecimal(parts[0]),
                    parts[1],
                    OffsetDateTime.parse(parts[2])
            );
        } catch (Exception e) {
            log.warn("Redis cache read failed for account {}: {}", accountId, e.getMessage());
            return null;
        }
    }

    public void putBalance(UUID accountId, BalanceResponse response) {
        try {
            String value = response.balance() + "|" + response.currency() + "|" + response.asOf();
            redisTemplate.opsForValue().set(KEY_PREFIX + accountId, value, TTL);
        } catch (Exception e) {
            log.warn("Redis cache write failed for account {}: {}", accountId, e.getMessage());
        }
    }

    public void evict(UUID accountId) {
        try {
            redisTemplate.delete(KEY_PREFIX + accountId);
        } catch (Exception e) {
            log.warn("Redis cache eviction failed for account {}: {}", accountId, e.getMessage());
        }
    }
}

