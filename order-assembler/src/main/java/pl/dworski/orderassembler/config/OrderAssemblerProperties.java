package pl.dworski.orderassembler.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("order-assembler")
public record OrderAssemblerProperties(@NotNull @Valid Topics topics) {

    public record Topics(
            @NotBlank String orders,
            @NotBlank String orderItems,
            @NotBlank String orderEvents) {
    }
}
