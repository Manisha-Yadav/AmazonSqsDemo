package com.message.endpoints;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.message.response.StandardMessageResponse;
import com.message.util.SQSUtility;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.nonNull;

/*
 * maxNumberOfMessages - Amazon SQS never returns more messages than this value (however, fewer messages might be returned).
 *
 * waitTimeSeconds - The length of time, in seconds, for which a ProcessMessage action waits for a message to arrive.
 *                   Valid values: An integer from 0 to 20 (seconds). Default: 0.
 */
public class ProcessMessage extends HttpServlet {

    public static final int VISIBILITY_INCREMENT = 10;

    private static final Map<String, String> TEMP_IN_MEMORY_STORAGE = new HashMap<>();
    public static final int BASE_TIME = 100;

    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, JsonProcessingException {

        String queueName = req.getParameter("queueName");
        Integer maxNumberOfMessages = Integer.parseInt(req.getParameter("maxNumberOfMessages"));
        Integer waitTimeSeconds = Integer.parseInt(req.getParameter("waitTimeSeconds"));
        Boolean visibilityTimeoutExtensionAllowed = Boolean.parseBoolean(req.getParameter("visibilityTimeoutExtensionAllowed"));

        String url = SQSUtility.createQueue(CreateQueueRequest.builder().queueName(queueName).build());
        List<Message> messages = SQSUtility.receiveMessages(ReceiveMessageRequest.builder()
                                                                .queueUrl(url)
                                                                .maxNumberOfMessages(maxNumberOfMessages)
                                                                // LONG POLLING 3 : MESSAGE RECEIPT
                                                                .waitTimeSeconds(waitTimeSeconds)
                                                                .build());

        List<StandardMessageResponse> httpResponse = new ArrayList<>();

        for (Message message : messages) {

            ScheduledExecutorService executor = null;
            if (visibilityTimeoutExtensionAllowed) {
                executor = Executors.newSingleThreadScheduledExecutor();
                Integer initialVisibilityTimeout = getVisibilityTimeout(url);
                VisibilityTimeoutState visibilityTimeoutState = new VisibilityTimeoutState(initialVisibilityTimeout);
                executor.scheduleWithFixedDelay(() -> extendVisibilityTimeout(url, message, VISIBILITY_INCREMENT, visibilityTimeoutState),
                                                initialVisibilityTimeout, VISIBILITY_INCREMENT, TimeUnit.SECONDS);
            }

            //process message
            Integer complexityFactor = Integer.parseInt(message.body().replaceAll("[^0-9]", ""));
            String requestCorrelationId = req.getHeader("RequestCorrelationId");
            processMessage(message.messageId(), complexityFactor, requestCorrelationId);

            //create response
            httpResponse.add(new StandardMessageResponse(message.messageId(),message.body(),queueName));

            //delete message
            SQSUtility.deleteMessage(DeleteMessageRequest.builder()
                                        .queueUrl(url)
                                        .receiptHandle(message.receiptHandle())
                                        .build());

            System.out.format("MESSAGE = %s ; COMPLEXITY = %d ; REQUEST CORRELATION ID = %s ; STATUS = DELETED; TIME = %s %n",
                    message.messageId(), complexityFactor, requestCorrelationId, LocalDateTime.now());

            if (nonNull(executor)) {
                System.out.format("Executor closed for message %s %n", message.messageId());
                executor.shutdownNow();
            }
        }

        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(httpResponse);

        try (PrintWriter writer = resp.getWriter()) {
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");
            resp.setStatus(201);
            writer.print(json);
            writer.flush();
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
            throw new ServletException(e);
        }

    }

    private void extendVisibilityTimeout(String url, Message m,
                                         int extendVisibilityTimeout, VisibilityTimeoutState visibilityTimeoutState) {
        System.out.println("Visibility extended at " + LocalDateTime.now());
        Integer visibilityTimeout = visibilityTimeoutState.getVisibilityTimeout();
        SQSUtility.changeMessagesVisibility(ChangeMessageVisibilityRequest.builder()
                                            .queueUrl(url)
                                            .receiptHandle(m.receiptHandle())
                                            .visibilityTimeout(visibilityTimeout)
                                            .build());
        visibilityTimeoutState.setVisibilityTimeout(visibilityTimeout + extendVisibilityTimeout);
        System.out.format("Visibility changed from %d s to %d s %n", visibilityTimeout,
                visibilityTimeoutState.getVisibilityTimeout());
    }

    private int getVisibilityTimeout(String url) {
        return Integer.parseInt(
                SQSUtility.getAttributesValueForQueue(url, QueueAttributeName.VISIBILITY_TIMEOUT));
    }

    private void processMessage(String messageId, Integer complexityFactor, String requestCorrelationId) {
        System.out.format("MESSAGE = %s ; COMPLEXITY = %d ; REQUEST CORRELATION ID = %s ; STATUS = STARTED; TIME = %s %n",
                messageId, complexityFactor, requestCorrelationId, LocalDateTime.now());
        try {
            if (!TEMP_IN_MEMORY_STORAGE.containsKey(messageId)) {
                Thread.sleep(complexityFactor * BASE_TIME);
                if (TEMP_IN_MEMORY_STORAGE.containsKey(messageId)) {
                    System.out.format("MESSAGE = %s ; COMPLEXITY = %d ; REQUEST CORRELATION ID = %s ; STATUS = ALREADY_PROCESSED; TIME = %s %n",
                            messageId, complexityFactor, TEMP_IN_MEMORY_STORAGE.get(messageId), LocalDateTime.now());
                }
                TEMP_IN_MEMORY_STORAGE.putIfAbsent(messageId, requestCorrelationId);
            } else {
                System.out.format("MESSAGE = %s ; COMPLEXITY = %d ; REQUEST CORRELATION ID = %s ; STATUS = ALREADY_PROCESSED; TIME = %s %n",
                        messageId, complexityFactor, TEMP_IN_MEMORY_STORAGE.get(messageId), LocalDateTime.now());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.format("MESSAGE = %s ; COMPLEXITY = %d ; REQUEST CORRELATION ID = %s ; STATUS = COMPLETED; TIME = %s %n",
                messageId, complexityFactor, requestCorrelationId, LocalDateTime.now());
    }
}

class VisibilityTimeoutState {
    private Integer visibilityTimeout;

    public VisibilityTimeoutState(Integer visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    public Integer getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public void setVisibilityTimeout(Integer visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }
}
