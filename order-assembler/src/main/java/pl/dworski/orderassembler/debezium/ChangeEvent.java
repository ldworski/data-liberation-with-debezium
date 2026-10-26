package pl.dworski.orderassembler.debezium;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ChangeEvent<T>(T before, T after, String op) {
}
