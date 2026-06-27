package com.kafka.system;

public class Transaction {
    private String transactionId;
    private String userId;
    private double amount;
    private String merchantId;
    private long timestamp;

    // Jackson requires a default constructor for deserialization
    public Transaction() {}

    public Transaction(String transactionId, String userId, double amount, String merchantId, long timestamp) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.merchantId = merchantId;
        this.timestamp = timestamp;
    }

    // Getters and Setters are required by Jackson
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public String getMerchantId() { return merchantId; }
    public void setMerchantId(String merchantId) { this.merchantId = merchantId; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return String.format("Transaction[ID=%s, User=%s, Amount=$%.2f, Merchant=%s]", 
                transactionId, userId, amount, merchantId);
    }
}