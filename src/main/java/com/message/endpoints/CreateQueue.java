package com.message.endpoints;

import com.message.util.SQSUtility;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.json.JSONObject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.nonNull;

/*
 * Visibility timeOut - a period of time during which Amazon SQS prevents other consumers from receiving and processing
 *  the message
 */

public class CreateQueue extends HttpServlet {

    public void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException {
        String queueName = null;
        String visibilityTimeOut = null;
        Boolean longPolling = false;
        String waitTimeSeconds = null;
        CreateQueueRequest.Builder builder = CreateQueueRequest.builder();
        Map<QueueAttributeName,String> queueAttributes = new HashMap<>();

        // 1. Fetch and parse body from HttpServletRequest
        StringBuilder sb = new StringBuilder();
        try (BufferedReader bufferedReader = req.getReader()) {
            String line = null;
            while (nonNull(line = bufferedReader.readLine())) {
                sb.append(line);
            }

            JSONObject jsonObject =  new JSONObject(sb.toString());
            queueName = jsonObject.getString("queueName");
            visibilityTimeOut = jsonObject.getString("visibilityTimeOut");
            longPolling = jsonObject.getBoolean("longPolling");
            waitTimeSeconds = jsonObject.getString("waitTimeSeconds");

            if (nonNull(queueName)) {
                builder.queueName(queueName);
            }

            if (nonNull(visibilityTimeOut)) {
                queueAttributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, visibilityTimeOut);
            }

            // LONG POLLING 1 : QUEUE CREATION.
            if (longPolling && nonNull(waitTimeSeconds)) {
                queueAttributes.put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, waitTimeSeconds);
            }

            if (!queueAttributes.keySet().isEmpty()) {
                builder.attributes(queueAttributes);
            }
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
            throw new ServletException(e);
        }

        String queueUrl = SQSUtility.createQueue(builder.build());

        // 4. Generate Response
        try (PrintWriter writer = resp.getWriter()) {
            resp.setContentType("application/text");
            resp.setCharacterEncoding("UTF-8");
            resp.setStatus(201);
            writer.format("Queue name %s created with visibilityTimeout %d with url %s",
                    queueName, visibilityTimeOut,queueUrl);
            writer.flush();
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
            throw new ServletException(e);
        }

    }
}
