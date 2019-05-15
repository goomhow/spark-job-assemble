package com.ctc.stream;

import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.Date;


public class MessageQQ implements Serializable {
    @SerializedName("message_id")
    private int messageId;
    @SerializedName("self_id")
    private long selfId;
    @SerializedName("user_id")
    private long userId;
    @SerializedName("message_type")
    private String messageType;
    @SerializedName("message")
    private String message;
    @SerializedName("time")
    private long messageTime;
    private long receiveTime = new Date().getTime();
    volatile private boolean isProcessed = false;

    public boolean isProcessed() {
        return isProcessed;
    }

    public void setProcessed() {
        isProcessed = true;
    }

    public MessageQQ(){}

    public MessageQQ(int messageId, long selfId, long userId, String messageType, String message, Long messageTime, Long receiveTime) {
        this.messageId = messageId;
        this.selfId = selfId;
        this.userId = userId;
        this.messageType = messageType;
        this.message = message;
        this.messageTime = messageTime > 946656000000L ? messageTime : messageTime*1000;
        this.receiveTime = receiveTime > 946656000000L ? receiveTime : receiveTime*1000;
    }



    public MessageQQ(int messageId, long selfId, long userId, String messageType, String message, Long messageTime) {
        this.messageId = messageId;
        this.selfId = selfId;
        this.userId = userId;
        this.messageType = messageType;
        this.message = message;
        this.messageTime = messageTime > 946656000000L ? messageTime : messageTime*1000;
    }

    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public long getSelfId() {
        return selfId;
    }

    public void setSelfId(long selfId) {
        this.selfId = selfId;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getMessageTime() {
        return messageTime;
    }

    public void setMessageTime(Long messageTime) {
        this.messageTime = messageTime > 946656000000L ? messageTime : messageTime*1000;
    }

    public Long getReceiveTime() {
        return receiveTime;
    }

    public void setReceiveTime(Long receiveTime) {
        this.receiveTime = receiveTime > 946656000000L ? receiveTime : receiveTime*1000;
    }

}
