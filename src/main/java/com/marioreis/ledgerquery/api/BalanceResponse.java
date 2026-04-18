package com.marioreis.ledgerquery.api;

import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Schema(description = "Current balance of an account")
public record BalanceResponse(
        @Schema(description = "Account ID") UUID accountId,
        @Schema(description = "Current balance", example = "1500.00") BigDecimal balance,
        @Schema(description = "Currency code", example = "USD") String currency,
        @Schema(description = "Timestamp of the last balance update") OffsetDateTime asOf
) {}

