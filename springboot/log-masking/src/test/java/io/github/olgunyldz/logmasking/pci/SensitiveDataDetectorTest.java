package io.github.olgunyldz.logmasking.pci;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

class SensitiveDataDetectorTest {

    private final JsonMapper mapper = new JsonMapper();
    private final SensitiveDataEncryption encryption = new SensitiveDataEncryption(SensitiveDataEncryptionTest.KEY);
    private final SensitiveDataDetector detector = new SensitiveDataDetector(encryption);

    private JsonNode json(String text) {
        return mapper.readTree(text);
    }

    @Test
    void removesSecurityFieldsAndEncryptsCardAndPersonalFields() {
        var input = json("""
                {"type":"PaymentAuthorized","payload":{
                  "card":{"number":"4111111111111111","cvv":"123","expiry":"12/27","holderName":"Jane Doe"},
                  "customer":{"email":"jane@example.com","phone":"+90 555 123 45 67"},
                  "amount":99.5}}""");

        var result = detector.protect(input);
        var card = result.node().at("/payload/card");
        var customer = result.node().at("/payload/customer");

        assertThat(card.has("cvv")).isFalse();
        assertThat(card.get("number").asString()).startsWith("pan:v1:1111:");
        assertThat(card.get("expiry").asString()).startsWith("exp:v1::");
        assertThat(customer.get("email").asString()).startsWith("email:v1:example.com:");
        assertThat(customer.get("phone").asString()).startsWith("phone:v1:67:");
        assertThat(encryption.decrypt(card.get("number").asString())).isEqualTo("4111111111111111");
        assertThat(encryption.decrypt(card.get("expiry").asString())).isEqualTo("12/27");
        assertThat(encryption.decrypt(card.get("holderName").asString())).isEqualTo("Jane Doe");
        assertThat(result.node().at("/payload/amount").asDouble()).isEqualTo(99.5);

        assertThat(result.removed()).containsExactly("payload.card.cvv (CVV)");
        assertThat(result.encrypted()).containsExactlyInAnyOrder(
                "payload.card.number (card number)",
                "payload.card.expiry (card expiry)",
                "payload.card.holderName (personal data)",
                "payload.customer.email (email)",
                "payload.customer.phone (phone)");
    }

    @Test
    void removesCredentialFields() {
        var input = json("""
                {"username":"ayse","password":"S3cret!","apiKey":"sk_live_51Hx9","session":{"accessToken":"eyJ.x.y"}}""");

        var result = detector.protect(input);

        assertThat(result.node().has("password")).isFalse();
        assertThat(result.node().has("apiKey")).isFalse();
        assertThat(result.node().at("/session").has("accessToken")).isFalse();
        assertThat(result.node().get("username").asString()).isEqualTo("ayse");
        assertThat(result.removed()).containsExactly(
                "password (credential)", "apiKey (credential)", "session.accessToken (credential)");
    }

    @Test
    void protectsEverythingInTheReferenceSupportTicket() {
        // The free-text sample from the reference course: every rule fires
        var ticket = "Refund request: Mastercard 5555-5555-5555-4444 cvc=999 expiry=03/28, Amex 378282246310005 cvv2: 7788. "
                + "Pay back to GB82 WEST 1234 5698 7654 32. Customer TCKN 10000000146, mehmet.demir@example.org, "
                + "mobile 0532 123 45 67. Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abc pin=4321";

        var result = detector.protect(json(mapper.writeValueAsString(java.util.Map.of("message", ticket))));
        String sent = result.node().get("message").asString();

        assertThat(sent).doesNotContain("5555", "999", "03/28", "378282", "7788", "GB82 WEST", "10000000146",
                "mehmet.demir", "0532", "eyJhbGci", "4321");
        assertThat(sent).contains("cvc=[REMOVED]", "cvv2: [REMOVED].", "Authorization: [REMOVED]", "pin=[REMOVED]",
                "pan:v1:4444:", "pan:v1:0005:", "iban:v1:GB5432:", "tckn:v1:46:", "exp:v1::",
                "email:v1:example.org:", "phone:v1:67:");
        assertThat(result.removed()).containsExactly("message (CVV)", "message (PIN)", "message (credential)");
        assertThat(result.encrypted()).containsExactly("message (IBAN)", "message (card number)", "message (national ID)",
                "message (card expiry)", "message (email)", "message (phone)");
        // Decrypting gives the text back, minus what was removed
        assertThat(encryption.decryptTokens(sent)).startsWith(
                "Refund request: Mastercard 5555-5555-5555-4444 cvc=[REMOVED] expiry=03/28, Amex 378282246310005");
    }

    @Test
    void leavesTheReferenceControlMessageAlone() {
        // Not a card (fails Luhn), a timestamp and a reference number that fails the TCKN check
        var input = json("{\"message\":\"Hello World! order 4111111111111112 shipped at 1793271793911, ref 12345678901\"}");

        var result = detector.protect(input);

        assertThat(result.findings()).isEmpty();
        assertThat(result.node()).isEqualTo(input);
    }

    @Test
    void encryptsCardNumbersStoredAsJsonNumbers() {
        var result = detector.protect(json("{\"pan\":4111111111111111,\"ref\":5555555555554444}"));

        assertThat(encryption.decrypt(result.node().get("pan").asString())).isEqualTo("4111111111111111");
        assertThat(encryption.decrypt(result.node().get("ref").asString())).isEqualTo("5555555555554444");
    }

    @Test
    void encryptsEveryValueUnderAPersonalObject() {
        var result = detector.protect(json("{\"address\":{\"street\":\"Ataturk Cd. 1\",\"city\":\"Istanbul\",\"zip\":34000}}"));

        assertThat(result.encrypted()).containsExactlyInAnyOrder(
                "address.street (personal data)", "address.city (personal data)", "address.zip (personal data)");
    }

    @Test
    void leavesOrdinaryMessagesUnchanged() {
        var input = json("""
                {"service":"order-service","name":"cpu_usage_percent","value":42,
                 "tags":{"instance":"pod-1"},"message":"Order 1234567890123456 shipped"}""");

        var result = detector.protect(input);

        assertThat(result.findings()).isEmpty();
        assertThat(result.node()).isEqualTo(input);
    }

    @Test
    void doesNotModifyTheInput() {
        var input = json("{\"cardNumber\":\"4111111111111111\"}");

        detector.protect(input);

        assertThat(input.get("cardNumber").asString()).isEqualTo("4111111111111111");
    }

    @Test
    void doesNotEncryptTwice() {
        var once = detector.protect(json("{\"message\":\"card 4111111111111111, call +90 555 123 45 67\"}")).node();
        var twice = detector.protect(once);

        assertThat(twice.findings()).isEmpty();
        assertThat(twice.node()).isEqualTo(once);
    }

    @Test
    void findsSensitiveDataInKeys() {
        assertThat(detector.findInText("4111111111111111")).containsExactly("card number");
        assertThat(detector.findInText("jane@example.com")).containsExactly("email");
        assertThat(detector.findInText("+90 555 123 45 67")).containsExactly("phone");
        assertThat(detector.findInText("order-42")).isEmpty();
    }
}
