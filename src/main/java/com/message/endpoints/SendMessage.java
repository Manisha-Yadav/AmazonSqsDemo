package com.message.endpoints;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.message.response.StandardMessageResponse;
import com.message.util.SQSUtility;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.json.JSONException;
import org.json.JSONObject;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import static java.util.Objects.nonNull;
import static software.amazon.awssdk.utils.StringUtils.isNotBlank;

public class SendMessage extends HttpServlet {

    private static final String EMPTY = "";

    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, JsonProcessingException {

        String queueName = null;
        String messageBody = null;
        String messageAttributeValue = null;
        String messageAttributeKey = null;
        String messageGroupId = null;
        String messageDeduplicationId = null;
        Integer delay = null;
        StandardMessageResponse httpResponse = null;

        StringBuilder sb = new StringBuilder();
        try (BufferedReader bufferedReader = req.getReader()) {
            String line = null;
            while (nonNull(line = bufferedReader.readLine())) {
                sb.append(line);
            }

            JSONObject jsonObject =  new JSONObject(sb.toString());
            queueName = jsonObject.getString("queueName");
            if (isNotBlank(queueName) && queueName.endsWith(".fifo")) {
                messageGroupId = jsonObject.getString("messageGroupId");
                messageDeduplicationId = getStringFromJsonIfPresent(jsonObject, "messageDeduplicationId");
            }
            messageBody = jsonObject.getString("messageBody");
            messageAttributeKey = getStringFromJsonIfPresent(jsonObject, "messageAttributeKey");
            messageAttributeValue = getStringFromJsonIfPresent(jsonObject, "messageAttributeValue");
            // delay - lets you postpone the delivery of new messages to a queue for a number of seconds
            delay = jsonObject.getInt("delay");

        } catch (IOException | JSONException e) {
            System.out.println(e.getLocalizedMessage());
            throw new ServletException(e);
        }

        String queueUrl = SQSUtility.createQueue(CreateQueueRequest.builder().queueName(queueName).build());

        /**
         * Message attributes are structured metadata (such as timestamps, geospatial data, signatures, and identifiers)
         * that are sent with the message.
         * Each message can have up to 10 attributes.
         * Message attributes are optional and separate from the message body.
         */
        Map<String , MessageAttributeValue> messageAttributeValueMap = new HashMap<>();

        if (isNotBlank(messageAttributeKey) && isNotBlank(messageAttributeValue)) {
            MessageAttributeValue value = MessageAttributeValue.builder()
                    .stringValue(messageAttributeValue).dataType("String")
                    .build();
            messageAttributeValueMap.put(messageAttributeKey, value);
        }

        SendMessageRequest.Builder builder = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .messageAttributes(messageAttributeValueMap)
                .delaySeconds(delay);
        if (nonNull(messageGroupId)) {
            builder.messageGroupId(messageGroupId);
            if (isNotBlank(messageDeduplicationId)) {
                builder.messageDeduplicationId(messageDeduplicationId);
            }
        }

        SendMessageResponse response = SQSUtility.sendMessage(builder.build());
        httpResponse = new StandardMessageResponse(response.messageId(),
                response.md5OfMessageBody(),queueName);

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(httpResponse);

        try (PrintWriter writer = resp.getWriter()) {
            resp.setContentType("application/text");
            resp.setCharacterEncoding("UTF-8");
            resp.setStatus(201);
            writer.print(json);
            writer.flush();
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
            throw new ServletException(e);
        }
    }

    private String getStringFromJsonIfPresent(JSONObject jsonObject, String key) {
        return jsonObject.has(key) && nonNull(jsonObject.get(key)) ?
                jsonObject.getString(key) : EMPTY;
    }
}
