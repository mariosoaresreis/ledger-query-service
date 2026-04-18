package com.marioreis.ledgerquery.api;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

@Schema(description = "Paginated list wrapper")
public record PagedResponse<T>(
        @Schema(description = "Page content") List<T> content,
        @Schema(description = "Current page (0-based)") int page,
        @Schema(description = "Page size") int size,
        @Schema(description = "Total number of elements") long totalElements,
        @Schema(description = "Total number of pages") int totalPages
) {}

