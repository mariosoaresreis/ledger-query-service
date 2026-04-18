package com.marioreis.ledgerquery.api;

import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Schema(description = "A single transaction entry")
public record TransactionResponse(
        @Schema(description = "Transaction ID") UUID id,
        @Schema(description = "Account ID") UUID accountId,
        @Schema(description = "Transaction amount (always positive)") BigDecimal amount,
        @Schema(description = "CREDIT or DEBIT", example = "CREDIT") String direction,
        @Schema(description = "Counterpart transfer UUID, if applicable") UUID counterpart,
        @Schema(description = "Reference string") String reference,
        @Schema(description = "Timestamp") OffsetDateTime occurredAt
) {}

