package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.*;
import java.util.Map;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:users_database.db";
        this.connection = DriverManager.getConnection(url);
        connection.createStatement().execute("CREATE TABLE users (" +
                "uuid VARCHAR(200) PRIMARY KEY, " +
                "email VARCHAR(200))");
    }

    public static void main(String[] args) throws SQLException {
        var createUserService = new CreateUserService();
        try(var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class,
                Map.of())) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, Order> record) throws InterruptedException, SQLException {
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for new user");
        System.out.println(record.value());

        var order = record.value();
        if(isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }

    }

    private void insertNewUser(String email) throws SQLException {
        PreparedStatement statement = this.connection.prepareStatement("INSERT INTO users (uuid, email) VALUES (?, ?)");
        statement.setString(1, "uuid");
        statement.setString(2, "email");
        statement.execute();

        System.out.println("User uuid");
    }

    private boolean isNewUser(String email) throws SQLException {
        PreparedStatement statement = this.connection.prepareStatement("SELECT uuid FROM users WHERE email = ? LIMIT 1");
        statement.setString(1, email);
        ResultSet resultSet = statement.executeQuery();

        return resultSet.next();
    }
}
