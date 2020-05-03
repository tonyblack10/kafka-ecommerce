package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class ServiceProvider {
    public <T> void run(ServiceFactory<T> factory) throws InterruptedException, ExecutionException, IOException {
        var emailService = factory.create();

        try (var service = new KafkaService(emailService.getConsumerGroup(),
                emailService.getTopic(),
                emailService::parse,
                Map.of())) {

            service.run();
        }
    }
}
