package pl.dworski.orderservice.adapter.out.persistence;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jdbc.test.autoconfigure.DataJdbcTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.jdbc.core.JdbcAggregateOperations;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;
import pl.dworski.orderservice.domain.model.Order;
import pl.dworski.orderservice.domain.model.OrderItem;
import pl.dworski.orderservice.domain.model.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataJdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(SpringDataOrderRepository.class)
@Testcontainers
class SpringDataOrderRepositoryTest {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("db/schema.sql"),
                    "/docker-entrypoint-initdb.d/init.sql");

    @Autowired
    private SpringDataOrderRepository repository;

    @Autowired
    private JdbcAggregateOperations aggregateOperations;

    @Test
    void savesOrderWithItems() {
        //given
        var keyboard = OrderItem.create("Mechanical Keyboard", 1, new BigDecimal("89.90"));
        var cable = OrderItem.create("USB-C Cable", 2, new BigDecimal("9.99"));
        var customerId = UUID.randomUUID();
        var order = Order.create(customerId, OrderStatus.PAID,
                Instant.now().truncatedTo(ChronoUnit.MICROS), List.of(keyboard, cable));

        //when
        repository.save(order);

        //then
        var saved = aggregateOperations.findById(order.orderNumber(), OrderEntity.class);
        assertThat(saved).isNotNull();
        assertThat(saved.orderNumber()).isEqualTo(order.orderNumber());
        assertThat(saved.customerId()).isEqualTo(customerId);
        assertThat(saved.status()).isEqualTo("PAID");
        assertThat(saved.createdAt()).isEqualTo(order.createdAt());
        assertThat(saved.items()).containsExactlyInAnyOrder(
                new OrderItemEntity(keyboard.id(), "Mechanical Keyboard", 1, new BigDecimal("89.90")),
                new OrderItemEntity(cable.id(), "USB-C Cable", 2, new BigDecimal("9.99")));
    }
}
