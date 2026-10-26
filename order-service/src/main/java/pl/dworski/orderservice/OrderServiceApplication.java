package pl.dworski.orderservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderServiceApplication {

    static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(OrderServiceApplication.class, args)));
    }

}
