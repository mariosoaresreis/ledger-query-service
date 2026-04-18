package com.marioreis.ledgerquery.api;

import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@Schema(description = "Monthly account statement")
public record StatementResponse(
        @Schema(description = "Account ID") UUID accountId,
        @Schema(description = "Statement month", example = "2024-11") String month,
        @Schema(description = "Opening balance at start of month") BigDecimal openingBalance,
        @Schema(description = "Closing balance at end of month") BigDecimal closingBalance,
        @Schema(description = "All transactions in the period") List<TransactionResponse> entries
) {}

