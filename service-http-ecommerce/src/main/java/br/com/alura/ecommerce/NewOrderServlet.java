package br.com.alura.ecommerce;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        try {
            orderDispatcher.close();
            emailDispatcher.close();
        } catch (IOException e) {
            // just logging
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            var email = req.getParameter("email");

            var orderId = UUID.randomUUID().toString();
            var amount = new BigDecimal(req.getParameter("amount"));
            var order = new Order(orderId, amount, email);

            orderDispatcher.send("ECOMMERCE_NEW_ORDER",
                    email,
                    new CorrelationId(NewOrderServlet.class.getSimpleName()),
                    order);

            var emailCode = new Email("New Order", "Thank you for your order! We are processing your order!");
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL",
                    email,
                    new CorrelationId(NewOrderServlet.class.getSimpleName()),
                    emailCode);

            System.out.println("New order sent successfully");

            resp.getWriter().println("New order sent");
            resp.setStatus(HttpServletResponse.SC_OK);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
            throw new ServletException(e);
        }
    }
}
