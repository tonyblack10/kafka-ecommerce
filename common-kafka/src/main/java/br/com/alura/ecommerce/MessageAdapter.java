package br.com.alura.ecommerce;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message> {
    @Override
    public JsonElement serialize(Message message, Type type, JsonSerializationContext context) {
        JsonObject object = new JsonObject();
        object.addProperty("type", message.getPayload().getClass().getName());
        object.add("payload", context.serialize(message.getPayload()));
        object.add("correlationId", context.serialize(message.getId()));

        return object;
    }
}
