package com.message.response;

public class StandardMessageResponse {

    String messageId;
    String messageBody;
    String queueName;

    public StandardMessageResponse(String messageId, String messageBody, String queueName) {
        this.messageId = messageId;
        this.messageBody = messageBody;
        this.queueName = queueName;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getMessageBody() {
        return messageBody;
    }
}
