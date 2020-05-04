package br.com.alura.ecommerce;

import br.com.alura.ecommerce.database.LocalDatabase;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        try {
            orderDispatcher.close();
        } catch (IOException e) {
            // just logging
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            var email = req.getParameter("email");
            var orderId = req.getParameter("uuid");
            var amount = new BigDecimal(req.getParameter("amount"));
            var order = new Order(orderId, amount, email);

            try (var database = new OrdersDatabase()) {
                if (database.saveNewOrder(order)) {


                    orderDispatcher.send("ECOMMERCE_NEW_ORDER",
                            email,
                            new CorrelationId(NewOrderServlet.class.getSimpleName()),
                            order);

                    System.out.println("New order sent successfully");
                    resp.getWriter().println("New order sent");
                } else {
                    System.out.println("Old order received");
                    resp.getWriter().println("Old order received");
                }
                resp.setStatus(HttpServletResponse.SC_OK);
            }
        } catch (ExecutionException | InterruptedException | SQLException e) {
            e.printStackTrace();
            throw new ServletException(e);
        }
    }
}
