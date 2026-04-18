package com.marioreis.ledgerquery.api;

import io.swagger.v3.oas.annotations.media.Schema;
import java.time.OffsetDateTime;
import java.util.UUID;

@Schema(description = "Account metadata summary")
public record AccountSummaryResponse(
        @Schema(description = "Account ID") UUID accountId,
        @Schema(description = "Owner ID") UUID ownerId,
        @Schema(description = "Account status", example = "ACTIVE") String status,
        @Schema(description = "Currency code", example = "USD") String currency,
        @Schema(description = "Creation timestamp") OffsetDateTime createdAt
) {}

