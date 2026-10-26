package pl.dworski.orderservice.adapter.in.cli;

import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties("order-generator")
public record OrderGeneratorProperties(
        @NotNull Duration interval,
        @NotNull Integer maxItems,
        @NotNull Integer maxQuantity,
        @NotNull Integer minUnitPrice,
        @NotNull Integer maxUnitPrice) {
}
