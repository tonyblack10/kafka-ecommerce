package br.com.alura.ecommerce;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            for (int i = 0; i < 10; i++) {
                var email = Math.random() + "@email.com";
                var orderId = UUID.randomUUID().toString();
                var amount = Math.random() * 5000 + 1;

                var id = new CorrelationId(NewOrderMain.class.getSimpleName());
                var order = new Order(orderId, new BigDecimal(amount), email);

                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, id, order);
            }
        }
    }
}
