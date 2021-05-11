package com.message.util;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQSUtility {

    private static final SqsClient sqsClient = SqsClient.builder()
            .region(Region.US_EAST_2)
            .build();

    /**
     * This method creates Queue OR returns the url of an existing queue
     * @return
     */

    public static String createQueue(CreateQueueRequest request){
        String url = null;
        try{
            url = sqsClient.createQueue(request).queueUrl();
        } catch (SqsException e){
            url = getQueueUrl(request.queueName());
        }
        return url;
    }

    public static String getQueueUrl(String queueName) {
        GetQueueUrlRequest queueUrlRequest = GetQueueUrlRequest.builder().queueName(queueName).build();
        return sqsClient.getQueueUrl(queueUrlRequest).queueUrl();
    }

    /**
     * Short polling occurs when the WaitTimeSeconds parameter of a ReceiveMessage request is set to 0
     * @param queueName
     * @param waitTimeSeconds
     */
    public static void enableLongPollingOnExistingQueue(String queueName, String waitTimeSeconds){

        String url = getQueueUrl(queueName);

        Map<QueueAttributeName,String> queueAttributes = new HashMap<>();
        queueAttributes.put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, waitTimeSeconds);

        // LONG POLLING 2 : EXISTING QUEUE
        SetQueueAttributesRequest setAttrsRequest = SetQueueAttributesRequest.builder()
                .queueUrl(url)
                .attributes(queueAttributes)
                .build();

        sqsClient.setQueueAttributes(setAttrsRequest);
    }

    public static List<Message> receiveMessages(ReceiveMessageRequest receiveMessageRequest) {
        try {
            return sqsClient.receiveMessage(receiveMessageRequest).messages();
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public static void linkDeadLetterQueue(String srcUrl , String deadLetterQueueUrl, Integer maxReceiveCount) {
        // 1. Get Queue ARN for the dead letter queue.
        GetQueueAttributesResponse queueAttrs = sqsClient.getQueueAttributes(
                GetQueueAttributesRequest.builder()
                        .queueUrl(deadLetterQueueUrl)
                        .attributeNames(QueueAttributeName.QUEUE_ARN).build());

        String dlQueueArn = queueAttrs.attributes().get(QueueAttributeName.QUEUE_ARN);

        // 2. Set REDRIVE_POLICY in src queue
        HashMap<QueueAttributeName, String> attributes = new HashMap<QueueAttributeName, String>();
        attributes.put(QueueAttributeName.REDRIVE_POLICY,
                "{\"maxReceiveCount\":\"" +
                    maxReceiveCount +
                        "\", \"deadLetterTargetArn\":\"" +
                            dlQueueArn +
                                "\"}");

        SetQueueAttributesRequest setAttrRequest = SetQueueAttributesRequest.builder()
                .queueUrl(srcUrl)
                .attributes(attributes)
                .build();

        sqsClient.setQueueAttributes(setAttrRequest);
    }

    public static void deleteMessage(DeleteMessageRequest deleteMessageRequest){
        try {
            sqsClient.deleteMessage(deleteMessageRequest);
        } catch (SqsException e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                throw e;
        }
    }

    public static SendMessageResponse sendMessage(SendMessageRequest sendMsgRequest){
        try {
            return sqsClient.sendMessage(sendMsgRequest);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    /*
    Visibility Timeout is a queue attribute and can be enabled/modified using SetQueueAttributesRequest
    But in this case, visibility timeout is for a specific message
    and hence use separate api changeMessageVisibility(ChangeMessageVisibilityRequest req)
     */
    public static void changeMessagesVisibility(ChangeMessageVisibilityRequest req) {
        System.out.println("Change Message Visibility for " + req.visibilityTimeout());
        try {
            sqsClient.changeMessageVisibility(req);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw e;
        }
    }

    public static String getAttributesValueForQueue(String queueUrl, QueueAttributeName attributeName){
        try{
            List<QueueAttributeName> atts = new ArrayList();
            atts.add(attributeName);

            GetQueueAttributesRequest attributesRequest= GetQueueAttributesRequest.builder()
                                                                .queueUrl(queueUrl)
                                                                .attributeNames(atts)
                                                                .build();
            GetQueueAttributesResponse response = sqsClient.getQueueAttributes(attributesRequest);

            Map<QueueAttributeName, String> queueAtts = response.attributes();

            for (Map.Entry<QueueAttributeName,String> queueAtt : queueAtts.entrySet()){
                    if(queueAtt.getKey().equals(attributeName)){
                        return queueAtt.getValue();
                    }
            }
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            throw e;
        }
        return null;
    }
}
