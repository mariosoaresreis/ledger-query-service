package com.marioreis.ledgerquery.projection;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Wire format of Kafka messages published by ledger-command-service.
 */
public record LedgerEventMessage(
        UUID eventId,
        UUID accountId,
        String eventType,
        OffsetDateTime occurredAt,
        String payload
) {}

