package pl.dworski.orderassembler.debezium;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public record OrderKey(@JsonProperty("order_number") UUID orderNumber) {
}
