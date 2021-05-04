package com.message.endpoints;

import com.fasterxml.jackson.core.JsonProcessingException;
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
 * When the ReceiveCount for a message exceeds the maxReceiveCount for a queue,
 * Amazon SQS moves the message to a dead-letter queue (with its original message ID).
 */
public class DeadLetterQueue extends HttpServlet {

    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, JsonProcessingException {

        String queueName = null;
        String queueVisibilityTimeout = null;
        String dlQueueName = null;
        String dlQueueVisibilityTimeout = null;

        StringBuilder sb = new StringBuilder();
        try (BufferedReader bufferedReader = req.getReader()) {
            String line = null;
            while (nonNull(line = bufferedReader.readLine())) {
                sb.append(line);
            }

            JSONObject jsonObject =  new JSONObject(sb.toString());
            queueName = jsonObject.getString("queueName");
            queueVisibilityTimeout = jsonObject.getString("queueVisibilityTimeout");
            dlQueueName = jsonObject.getString("dlQueueName");
            dlQueueVisibilityTimeout = jsonObject.getString("dlQueueVisibilityTimeout");
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
            throw new ServletException(e);
        }
        
        String srcQueueUrl = SQSUtility.createQueue(setUpRequest(queueName, queueVisibilityTimeout));
        String dlQueueUrl = SQSUtility.createQueue(setUpRequest(dlQueueName, dlQueueVisibilityTimeout));

        SQSUtility.linkDeadLetterQueue(srcQueueUrl, dlQueueUrl, 5);

        try (PrintWriter writer = resp.getWriter()) {
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            resp.setStatus(201);
            writer.print("Dead Letter Queue created and added to source queue");
            writer.flush();
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
            throw new ServletException(e);
        }

    }

    private CreateQueueRequest setUpRequest(String queueName, String queueVisibilityTimeout) {
        CreateQueueRequest.Builder queueBuilder = CreateQueueRequest.builder();
        Map<QueueAttributeName,String> queueAttributes = new HashMap<>();

        if (nonNull(queueName)) {
            queueBuilder.queueName(queueName);
        }

        if (nonNull(queueVisibilityTimeout)) {
            queueAttributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, queueVisibilityTimeout);
            queueBuilder.attributes(queueAttributes);
        }
        return queueBuilder.build();
    }


}
