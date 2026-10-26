package pl.dworski.orderassembler.debezium;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderItemRow(
        @JsonProperty("id") UUID id,
        @JsonProperty("order_number") UUID orderNumber,
        @JsonProperty("item_name") String itemName,
        @JsonProperty("quantity") int quantity,
        @JsonProperty("unit_price") BigDecimal unitPrice) {
}
