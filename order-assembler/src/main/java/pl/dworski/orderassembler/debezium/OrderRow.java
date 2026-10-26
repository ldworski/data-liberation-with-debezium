package pl.dworski.orderassembler.debezium;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderRow(
        @JsonProperty("order_number") UUID orderNumber,
        @JsonProperty("customer_id") UUID customerId,
        @JsonProperty("status") String status,
        @JsonProperty("created_at") Instant createdAt) {
}
