package io.github.olgunyldz.producer.api;

import io.github.olgunyldz.producer.config.TopicProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Request bodies with card and personal data, for trying out the PCI protection from the
 * web page. All card numbers are public test numbers, and all people and IDs are made up.
 */
@Component
public class SampleMessages {

    /**
     * @param id     stable id for the page
     * @param label  button text
     * @param topic  the topic this body is sent to
     * @param expect what the protection should do with it
     * @param body   the request body, as the matching POST endpoint expects it
     */
    public record Sample(String id, String label, String topic, String expect, Map<String, Object> body) {
    }

    private final List<Sample> samples;

    public SampleMessages(TopicProperties topics) {
        this.samples = List.of(
                new Sample("declined-card-log", "Log: declined card", topics.logs(),
                        "Card number and email encrypted inside the text, CVV removed",
                        Map.of("service", "payment-service",
                                "level", "ERROR",
                                "message", "Payment declined for card 4111 1111 1111 1111, CVV 123, customer jane.doe@example.com")),

                new Sample("support-ticket-log", "Log: support ticket", topics.logs(),
                        "Every rule in free text: cards, CVC, CVV2, expiry, IBAN, national ID, email, mobile, Bearer token, PIN",
                        Map.of("service", "support-service",
                                "level", "WARN",
                                "message", "Refund request: Mastercard 5555-5555-5555-4444 cvc=999 expiry=03/28, "
                                        + "Amex 378282246310005 cvv2: 7788. Pay back to GB82 WEST 1234 5698 7654 32. "
                                        + "Customer TCKN 10000000146, mehmet.demir@example.org, mobile 0532 123 45 67. "
                                        + "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abc pin=4321")),

                new Sample("refund-log", "Log: refund to IBAN", topics.logs(),
                        "IBAN and national ID encrypted inside the text",
                        Map.of("service", "payment-service",
                                "level", "INFO",
                                "message", "Refund of 49.90 EUR sent to TR330006100519786457841326 for customer 10000000146")),

                new Sample("card-payment-event", "Event: card payment", topics.events(),
                        "Card number, expiry and holder name encrypted, CVV removed, amount unchanged",
                        Map.of("type", "PaymentAuthorized",
                                "aggregateId", "order-1001",
                                "payload", Map.of(
                                        "amount", 129.90,
                                        "currency", "EUR",
                                        "card", Map.of(
                                                "number", "5555555555554444",
                                                "cvv", "737",
                                                "expiry", "03/28",
                                                "holderName", "Jane Doe")))),

                new Sample("card-swipe-event", "Event: card swipe", topics.events(),
                        "Track 2 data and PIN removed",
                        Map.of("type", "CardPresentPayment",
                                "aggregateId", "order-1002",
                                "payload", Map.of(
                                        "terminalId", "POS-7",
                                        "track2", ";4111111111111111=25121010000000000?",
                                        "pin", "1234",
                                        "amount", 42.00))),

                new Sample("customer-event", "Event: new customer", topics.events(),
                        "Every personal field encrypted, including the whole address",
                        Map.of("type", "CustomerRegistered",
                                "aggregateId", "customer-77",
                                "payload", Map.of(
                                        "customer", Map.of(
                                                "firstName", "Jane",
                                                "lastName", "Doe",
                                                "email", "jane.doe@example.com",
                                                "phone", "+90 555 123 45 67",
                                                "birthDate", "1990-04-12",
                                                "nationalId", "10000000146",
                                                "address", Map.of("street", "Ataturk Cd. 1", "city", "Istanbul")),
                                        "marketingOptIn", true))),

                new Sample("login-event", "Event: login with secrets", topics.events(),
                        "Password, API key and tokens removed, username kept",
                        Map.of("type", "UserLoggedIn",
                                "aggregateId", "user-5",
                                "payload", Map.of(
                                        "username", "ayse",
                                        "password", "S3cret!",
                                        "apiKey", "sk_live_51Hx9",
                                        "session", Map.of(
                                                "accessToken", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJheXNlIn0.sig",
                                                "refreshToken", "rt_8f2a91",
                                                "expiresIn", 3600)))),

                new Sample("sensitive-key", "Event: card number as key", topics.events(),
                        "Rejected with 400: a key can't be encrypted without breaking partitioning",
                        Map.of("type", "PaymentAuthorized",
                                "aggregateId", "4111111111111111",
                                "payload", Map.of("amount", 10))),

                new Sample("clean-log", "Log: nothing sensitive", topics.logs(),
                        "Sent unchanged: 4111111111111112 fails the Luhn check, the timestamp and ref aren't cards or IDs",
                        Map.of("service", "order-service",
                                "level", "INFO",
                                "message", "Hello World! order 4111111111111112 shipped at 1793271793911, ref 12345678901")),

                new Sample("clean-metric", "Metric: nothing sensitive", topics.metrics(),
                        "Sent unchanged",
                        Map.of("service", "payment-service",
                                "name", "payment_amount_eur",
                                "value", 129.90,
                                "tags", Map.of("currency", "EUR", "instance", "pod-1"))));
    }

    public List<Sample> all() {
        return samples;
    }
}
