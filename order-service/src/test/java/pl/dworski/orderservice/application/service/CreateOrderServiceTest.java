package pl.dworski.orderservice.application.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.dworski.orderservice.application.port.in.CreateOrderUseCase.CreateOrderCommand;
import pl.dworski.orderservice.application.port.out.SaveOrderPort;
import pl.dworski.orderservice.domain.model.Order;
import pl.dworski.orderservice.domain.model.OrderStatus;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class CreateOrderServiceTest {

    private static final UUID CUSTOMER_ID = UUID.fromString("acdec8ac-fa75-4eae-96dd-7a01c621e9e4");
    private static final Instant NOW = Instant.parse("2026-10-25T12:00:00Z");

    @Mock
    private SaveOrderPort saveOrderPort;

    @Captor
    private ArgumentCaptor<Order> orderArgumentCaptor;

    private CreateOrderService createOrderService() {
        return new CreateOrderService(saveOrderPort, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void createsOrderFromCommandAndSavesIt() {
        //given
        var command = new CreateOrderCommand(CUSTOMER_ID, OrderStatus.NEW, List.of(
                new CreateOrderCommand.Item("Mechanical Keyboard", 1, new BigDecimal("89.90")),
                new CreateOrderCommand.Item("USB-C Cable", 2, new BigDecimal("9.99"))));

        //when
        var order = createOrderService().create(command);

        //then
        verify(saveOrderPort).save(orderArgumentCaptor.capture());
        assertThat(orderArgumentCaptor.getValue()).isEqualTo(order);
        assertThat(order.orderNumber()).isNotNull();
        assertThat(order.customerId()).isEqualTo(CUSTOMER_ID);
        assertThat(order.status()).isEqualTo(OrderStatus.NEW);
        assertThat(order.createdAt()).isEqualTo(NOW);
        assertThat(order.items())
                .extracting("itemName", "quantity", "unitPrice")
                .containsExactly(
                        tuple("Mechanical Keyboard", 1, new BigDecimal("89.90")),
                        tuple("USB-C Cable", 2, new BigDecimal("9.99")));
        assertThat(order.items()).allSatisfy(item -> assertThat(item.id()).isNotNull());
    }

    @Test
    void doesNotSaveOrderViolatingDomainRules() {
        //given
        var command = new CreateOrderCommand(CUSTOMER_ID, OrderStatus.PAID, List.of());

        //when & then
        assertThatThrownBy(() -> createOrderService().create(command))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(saveOrderPort);
    }
}
