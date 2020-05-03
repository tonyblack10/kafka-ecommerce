package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class EmailService implements ConsumerService<Email> {

    public static void main(String[] args) throws InterruptedException, ExecutionException, IOException {
        new ServiceProvider().run(EmailService::new);
    }

    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    public void parse(ConsumerRecord<String, Message<Email>> record) {
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
