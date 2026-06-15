package training.temporal.choreography;

public record DomainEvent(String orderId, String type, String payload) {}

