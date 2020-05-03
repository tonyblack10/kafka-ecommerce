package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Map;

public class EmailService {

    public static void main(String[] args) throws IOException {
        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Email.class,
                Map.of())) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Message<Email>> record) {
        System.out.println("------------------------------------------");
        System.out.println("Send e-mail:");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignoring...
            e.printStackTrace();
        }

        System.out.println("E-mail sent!");
    }

}
