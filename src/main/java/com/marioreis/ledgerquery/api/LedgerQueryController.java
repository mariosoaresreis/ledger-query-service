package com.marioreis.ledgerquery.api;

import com.marioreis.ledgerquery.persistence.ProjectionRepository;
import com.marioreis.ledgerquery.service.BalanceCacheService;
import com.marioreis.ledgerquery.service.KafkaLagService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Ledger Queries", description = "CQRS read-side endpoints for account balances, transactions, and statements")
public class LedgerQueryController {

    private final ProjectionRepository projectionRepository;
    private final BalanceCacheService balanceCacheService;
    private final KafkaLagService kafkaLagService;

    public LedgerQueryController(ProjectionRepository projectionRepository,
                                 BalanceCacheService balanceCacheService,
                                 KafkaLagService kafkaLagService) {
        this.projectionRepository = projectionRepository;
        this.balanceCacheService = balanceCacheService;
        this.kafkaLagService = kafkaLagService;
    }

    // ── Balance ───────────────────────────────────────────────────────────────

    @GetMapping("/accounts/{accountId}/balance")
    @Operation(summary = "Get account balance", description = "Returns current balance. Served from Redis cache when available.")
    public ResponseEntity<BalanceResponse> getBalance(@PathVariable UUID accountId) {
        // Try Redis cache first
        BalanceResponse cached = balanceCacheService.getBalance(accountId);
        if (cached != null) {
            return ResponseEntity.ok(cached);
        }

        Map<String, Object> row = projectionRepository.findBalance(accountId)
                .orElseThrow(() -> new QueryNotFoundException("Account %s not found".formatted(accountId)));

        BalanceResponse response = new BalanceResponse(
                accountId,
                (BigDecimal) row.get("balance"),
                (String) row.get("currency"),
                toOffsetDateTime(row.get("updated_at"))
        );
        balanceCacheService.putBalance(accountId, response);
        return ResponseEntity.ok(response);
    }

    // ── Transactions ──────────────────────────────────────────────────────────

    @GetMapping("/accounts/{accountId}/transactions")
    @Operation(summary = "Get transaction history", description = "Paginated transaction history with optional date and direction filters.")
    public ResponseEntity<PagedResponse<TransactionResponse>> getTransactions(
            @PathVariable UUID accountId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
            @RequestParam(required = false) String direction) {

        OffsetDateTime fromDt = from != null ? from.atStartOfDay().atOffset(ZoneOffset.UTC) : null;
        OffsetDateTime toDt   = to   != null ? to.plusDays(1).atStartOfDay().atOffset(ZoneOffset.UTC) : null;
        int offset = page * size;

        List<Map<String, Object>> rows = projectionRepository.findTransactions(accountId, fromDt, toDt, direction, offset, size);
        long total = projectionRepository.countTransactions(accountId, fromDt, toDt, direction);

        List<TransactionResponse> content = rows.stream().map(this::mapTransaction).toList();
        int totalPages = (int) Math.ceil((double) total / size);
        return ResponseEntity.ok(new PagedResponse<>(content, page, size, total, totalPages));
    }

    // ── Statement ─────────────────────────────────────────────────────────────

    @GetMapping("/accounts/{accountId}/statement")
    @Operation(summary = "Get monthly statement", description = "Monthly statement with opening balance, entries, and closing balance.")
    public ResponseEntity<StatementResponse> getStatement(
            @PathVariable UUID accountId,
            @RequestParam String month) {  // format: 2024-11

        YearMonth ym = YearMonth.parse(month);
        OffsetDateTime monthStart = ym.atDay(1).atStartOfDay().atOffset(ZoneOffset.UTC);
        OffsetDateTime monthEnd   = ym.atEndOfMonth().plusDays(1).atStartOfDay().atOffset(ZoneOffset.UTC);

        List<Map<String, Object>> rows = projectionRepository.findTransactions(
                accountId, monthStart, monthEnd, null, 0, Integer.MAX_VALUE);

        List<TransactionResponse> entries = rows.stream().map(this::mapTransaction).toList();

        // Compute opening balance = current balance − sum of month entries
        Map<String, Object> balanceRow = projectionRepository.findBalance(accountId)
                .orElseThrow(() -> new QueryNotFoundException("Account %s not found".formatted(accountId)));
        BigDecimal currentBalance = (BigDecimal) balanceRow.get("balance");

        BigDecimal monthNet = entries.stream()
                .map(tx -> "CREDIT".equals(tx.direction()) ? tx.amount() : tx.amount().negate())
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        BigDecimal openingBalance = currentBalance.subtract(monthNet);

        return ResponseEntity.ok(new StatementResponse(accountId, month, openingBalance, currentBalance, entries));
    }

    // ── Raw event log ─────────────────────────────────────────────────────────

    @GetMapping("/accounts/{accountId}/events")
    @Operation(summary = "Get raw event log", description = "Full audit trail of all events for an account.")
    public ResponseEntity<List<Map<String, Object>>> getEvents(@PathVariable UUID accountId) {
        return ResponseEntity.ok(projectionRepository.findEventLog(accountId));
    }

    // ── Accounts by owner ─────────────────────────────────────────────────────

    @GetMapping("/accounts")
    @Operation(summary = "List accounts by owner", description = "Returns all accounts for a given ownerId.")
    public ResponseEntity<List<AccountSummaryResponse>> getAccountsByOwner(
            @RequestParam UUID ownerId) {
        List<Map<String, Object>> rows = projectionRepository.findAccountsByOwner(ownerId);
        List<AccountSummaryResponse> result = rows.stream().map(r -> new AccountSummaryResponse(
                toUUID(r.get("account_id")),
                toUUID(r.get("owner_id")),
                (String) r.get("status"),
                (String) r.get("currency"),
                toOffsetDateTime(r.get("created_at"))
        )).toList();
        return ResponseEntity.ok(result);
    }

    // ── Admin: replay ─────────────────────────────────────────────────────────

    @PostMapping("/admin/replay/{accountId}")
    @Operation(summary = "Re-project balance for an account", description = "Recomputes the account balance from transaction_history. Use for recovery.")
    public ResponseEntity<Void> replay(@PathVariable UUID accountId) {
        projectionRepository.replayBalance(accountId);
        balanceCacheService.evict(accountId);
        return ResponseEntity.accepted().build();
    }

    // ── Health: Kafka lag ─────────────────────────────────────────────────────

    @GetMapping("/health/lag")
    @Operation(summary = "Kafka consumer lag", description = "Returns per-partition consumer group lag for the projection consumer.")
    public ResponseEntity<Map<String, Object>> getKafkaLag() {
        return ResponseEntity.ok(kafkaLagService.getLag());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private TransactionResponse mapTransaction(Map<String, Object> row) {
        return new TransactionResponse(
                toUUID(row.get("id")),
                toUUID(row.get("account_id")),
                (BigDecimal) row.get("amount"),
                (String) row.get("direction"),
                row.get("counterpart") != null ? toUUID(row.get("counterpart")) : null,
                (String) row.get("reference"),
                toOffsetDateTime(row.get("occurred_at"))
        );
    }

    private UUID toUUID(Object value) {
        if (value instanceof UUID u) return u;
        return UUID.fromString(value.toString());
    }

    private OffsetDateTime toOffsetDateTime(Object value) {
        if (value instanceof OffsetDateTime odt) return odt;
        if (value instanceof java.sql.Timestamp ts) return ts.toInstant().atOffset(ZoneOffset.UTC);
        return null;
    }
}

