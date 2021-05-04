package com.message.endpoints;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.message.response.StandardMessageResponse;
import com.message.util.SQSUtility;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.json.JSONObject;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.nonNull;
/*
 *  delay - lets you postpone the delivery of new messages to a queue for a number of seconds
 */
public class SendMessage extends HttpServlet {

    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, JsonProcessingException {

        String queueName = null;
        String messageBody = null;
        String messageAttributeValue = null;
        String messageAttributeKey = null;
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
            messageBody = jsonObject.getString("messageBody");
            messageAttributeKey = jsonObject.getString("messageAttributeKey");
            messageAttributeValue = jsonObject.getString("messageAttributeValue");
            delay = jsonObject.getInt("delay");

        } catch (IOException e) {
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
        MessageAttributeValue value = MessageAttributeValue.builder()
                .stringValue(messageAttributeValue).dataType("String")
                .build();
        messageAttributeValueMap.put(messageAttributeKey,value);

        SendMessageResponse response = SQSUtility.sendMessage(SendMessageRequest.builder()
                                                                .queueUrl(queueUrl)
                                                                .messageBody(messageBody)
                                                                .messageAttributes(messageAttributeValueMap)
                                                                .delaySeconds(delay)
                                                                .build());
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
}
