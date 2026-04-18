package com.marioreis.ledgerquery.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ProjectionRepository {

    private final JdbcTemplate jdbc;

    public ProjectionRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // ── Event log ────────────────────────────────────────────────────────────

    public boolean eventLogExists(UUID eventId) {
        Integer count = jdbc.queryForObject(
                "select count(1) from event_log where id = ?", Integer.class, eventId);
        return count != null && count > 0;
    }

    @Transactional
    public void insertEventLog(UUID eventId, UUID accountId, String eventType,
                               String payload, OffsetDateTime occurredAt) {
        jdbc.update("""
                insert into event_log (id, account_id, event_type, payload, occurred_at)
                values (?, ?, ?, cast(? as jsonb), ?)
                on conflict (id) do nothing
                """,
                eventId, accountId, eventType, payload, occurredAt);
    }

    // ── Account summary ──────────────────────────────────────────────────────

    @Transactional
    public void upsertAccountSummary(UUID accountId, UUID ownerId, String status,
                                     String currency, OffsetDateTime createdAt) {
        jdbc.update("""
                insert into account_summary (account_id, owner_id, status, currency, created_at, updated_at)
                values (?, ?, ?, ?, ?, ?)
                on conflict (account_id) do update
                  set status = excluded.status, updated_at = excluded.updated_at
                """,
                accountId, ownerId, status, currency, createdAt, createdAt);
    }

    @Transactional
    public void updateAccountStatus(UUID accountId, String status, OffsetDateTime updatedAt) {
        jdbc.update("""
                update account_summary set status = ?, updated_at = ?
                where account_id = ?
                """,
                status, updatedAt, accountId);
    }

    public Optional<Map<String, Object>> findAccountSummary(UUID accountId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from account_summary where account_id = ?", accountId);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    public List<Map<String, Object>> findAccountsByOwner(UUID ownerId) {
        return jdbc.queryForList(
                "select * from account_summary where owner_id = ? order by created_at asc", ownerId);
    }

    // ── Account balances ─────────────────────────────────────────────────────

    @Transactional
    public void upsertAccountBalance(UUID accountId, BigDecimal balance,
                                     String currency, UUID lastEventId, OffsetDateTime updatedAt) {
        jdbc.update("""
                insert into account_balances (account_id, balance, currency, last_event_id, updated_at)
                values (?, ?, ?, ?, ?)
                on conflict (account_id) do update
                  set balance = excluded.balance, last_event_id = excluded.last_event_id,
                      updated_at = excluded.updated_at
                """,
                accountId, balance, currency, lastEventId, updatedAt);
    }

    @Transactional
    public void adjustBalance(UUID accountId, BigDecimal delta, UUID lastEventId, OffsetDateTime updatedAt) {
        jdbc.update("""
                update account_balances
                set balance = balance + ?,
                    last_event_id = ?,
                    updated_at = ?
                where account_id = ?
                """,
                delta, lastEventId, updatedAt, accountId);
    }

    public Optional<Map<String, Object>> findBalance(UUID accountId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "select * from account_balances where account_id = ?", accountId);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }

    // ── Transaction history ──────────────────────────────────────────────────

    @Transactional
    public void insertTransaction(UUID id, UUID accountId, BigDecimal amount, String direction,
                                  UUID counterpart, String reference, OffsetDateTime occurredAt) {
        jdbc.update("""
                insert into transaction_history (id, account_id, amount, direction, counterpart, reference, occurred_at)
                values (?, ?, ?, ?, ?, ?, ?)
                on conflict (id) do nothing
                """,
                id, accountId, amount, direction, counterpart, reference, occurredAt);
    }

    public List<Map<String, Object>> findTransactions(UUID accountId, OffsetDateTime from, OffsetDateTime to,
                                                       String direction, int offset, int limit) {
        StringBuilder sql = new StringBuilder("""
                select * from transaction_history
                where account_id = ?
                """);
        List<Object> params = new java.util.ArrayList<>();
        params.add(accountId);

        if (from != null) {
            sql.append(" and occurred_at >= ?");
            params.add(from);
        }
        if (to != null) {
            sql.append(" and occurred_at <= ?");
            params.add(to);
        }
        if (direction != null) {
            sql.append(" and direction = ?");
            params.add(direction.toUpperCase());
        }
        sql.append(" order by occurred_at desc limit ? offset ?");
        params.add(limit);
        params.add(offset);

        return jdbc.queryForList(sql.toString(), params.toArray());
    }

    public long countTransactions(UUID accountId, OffsetDateTime from, OffsetDateTime to, String direction) {
        StringBuilder sql = new StringBuilder("select count(1) from transaction_history where account_id = ?");
        List<Object> params = new java.util.ArrayList<>();
        params.add(accountId);
        if (from != null)      { sql.append(" and occurred_at >= ?"); params.add(from); }
        if (to != null)        { sql.append(" and occurred_at <= ?"); params.add(to); }
        if (direction != null) { sql.append(" and direction = ?");    params.add(direction.toUpperCase()); }
        Long count = jdbc.queryForObject(sql.toString(), Long.class, params.toArray());
        return count == null ? 0L : count;
    }

    // ── Event log queries ────────────────────────────────────────────────────

    public List<Map<String, Object>> findEventLog(UUID accountId) {
        return jdbc.queryForList("""
                select id, account_id, event_type, cast(payload as varchar) as payload, occurred_at
                from event_log
                where account_id = ?
                order by occurred_at asc
                """, accountId);
    }

    // ── Replay: recompute balance from transaction history ───────────────────

    @Transactional
    public void replayBalance(UUID accountId) {
        jdbc.update("""
                update account_balances
                set balance = (
                    select coalesce(
                        sum(case when direction = 'CREDIT' then amount else -amount end),
                        0
                    )
                    from transaction_history where account_id = ?
                ),
                updated_at = now()
                where account_id = ?
                """, accountId, accountId);
    }
}

