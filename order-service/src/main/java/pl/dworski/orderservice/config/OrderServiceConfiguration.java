package pl.dworski.orderservice.config;

import net.datafaker.Faker;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import pl.dworski.orderservice.adapter.in.cli.GenerateOrdersCommandLineRunner;
import pl.dworski.orderservice.adapter.in.cli.OrderGeneratorProperties;
import pl.dworski.orderservice.adapter.in.cli.RandomOrdersGenerator;
import pl.dworski.orderservice.application.port.in.CreateOrderUseCase;
import pl.dworski.orderservice.application.port.out.SaveOrderPort;
import pl.dworski.orderservice.application.service.CreateOrderService;

import java.time.Clock;

@Configuration
@EnableConfigurationProperties(OrderGeneratorProperties.class)
class OrderServiceConfiguration {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    CreateOrderUseCase createOrderUseCase(SaveOrderPort saveOrderPort, Clock clock) {
        return new CreateOrderService(saveOrderPort, clock);
    }

    @Bean
    RandomOrdersGenerator randomOrdersGenerator(OrderGeneratorProperties properties) {
        return new RandomOrdersGenerator(new Faker(), properties);
    }

    @Bean
    GenerateOrdersCommandLineRunner generateOrdersCommandLineRunner(
            CreateOrderUseCase createOrderUseCase,
            RandomOrdersGenerator randomOrdersGenerator,
            OrderGeneratorProperties properties) {
        return new GenerateOrdersCommandLineRunner(createOrderUseCase, randomOrdersGenerator, properties.interval(), System.in);
    }
}
