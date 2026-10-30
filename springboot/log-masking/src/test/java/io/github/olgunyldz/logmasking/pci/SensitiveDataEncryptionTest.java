package io.github.olgunyldz.logmasking.pci;

import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SensitiveDataEncryptionTest {

    static final String KEY = "+WpfE866Ysy6Lp51Q61eBAcupVyUtI5nnyxwm6avujc=";
    static final String OTHER_KEY = Base64.getEncoder().encodeToString(new byte[32]);

    private final SensitiveDataEncryption encryption = new SensitiveDataEncryption(KEY);

    @Test
    void roundTrips() {
        String token = encryption.encrypt(Type.PAN, "4111111111111111");

        assertThat(token).startsWith("pan:v1:1111:").doesNotContain("411111");
        assertThat(SensitiveDataEncryption.isEncrypted(token)).isTrue();
        assertThat(encryption.decrypt(token)).isEqualTo("4111111111111111");
    }

    @ParameterizedTest
    @CsvSource({
            "PAN, 4111 1111 1111 1111, pan:v1:1111:",
            "IBAN, TR330006100519786457841326, iban:v1:TR1326:",
            "TCKN, 10000000146, tckn:v1:46:",
            "EXPIRY, 12/27, exp:v1::",
            "EMAIL, jane.doe@example.com, email:v1:example.com:",
            "PHONE, +90 555 123 45 67, phone:v1:67:",
            "PERSONAL, Jane Doe, pii:v1::"})
    void tokensCarryTheirTypeAndAReadableHint(Type type, String value, String prefix) {
        String token = encryption.encrypt(type, value);

        assertThat(token).startsWith(prefix);
        assertThat(encryption.decrypt(token)).isEqualTo(value);
    }

    @Test
    void sameValueGivesDifferentTokens() {
        // A random IV per value: equal plaintexts can't be spotted by comparing ciphertexts
        assertThat(encryption.encrypt(Type.EMAIL, "jane@example.com"))
                .isNotEqualTo(encryption.encrypt(Type.EMAIL, "jane@example.com"));
    }

    @Test
    void anotherKeyCannotDecrypt() {
        String token = encryption.encrypt(Type.PERSONAL, "secret");

        assertThatThrownBy(() -> new SensitiveDataEncryption(OTHER_KEY).decrypt(token))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void modifiedPayloadIsRejected() {
        String token = encryption.encrypt(Type.PERSONAL, "secret");
        // Change a character in the middle of the payload: the last base64 character may only hold padding bits
        int i = token.length() - 20;
        String tampered = token.substring(0, i) + (token.charAt(i) == 'A' ? 'B' : 'A') + token.substring(i + 1);

        assertThatThrownBy(() -> encryption.decrypt(tampered)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void typeAndHintCannotBeChanged() {
        // The header is authenticated data: a fake hint or type breaks decryption
        String token = encryption.encrypt(Type.PAN, "4111111111111111");

        assertThatThrownBy(() -> encryption.decrypt(token.replace("pan:v1:1111:", "pan:v1:9999:")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> encryption.decrypt(token.replace("pan:v1:1111:", "pii:v1:1111:")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decryptsTokensInsideText() {
        String text = "card " + encryption.encrypt(Type.PAN, "4111111111111111")
                + " for " + encryption.encrypt(Type.EMAIL, "jane@example.com");

        assertThat(SensitiveDataEncryption.containsToken(text)).isTrue();
        assertThat(encryption.decryptTokens(text)).isEqualTo("card 4111111111111111 for jane@example.com");
    }

    @Test
    void neverReturnsAHalfDecryptedText() {
        String foreign = new SensitiveDataEncryption(OTHER_KEY).encrypt(Type.PERSONAL, "x");
        String text = encryption.encrypt(Type.PERSONAL, "mine") + " and " + foreign;

        assertThatThrownBy(() -> encryption.decryptTokens(text)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void textWithoutTokensIsReturnedAsIs() {
        assertThat(encryption.decryptTokens("ratio 1:v1:2")).isEqualTo("ratio 1:v1:2");
        assertThat(encryption.decryptTokens(null)).isNull();
    }

    @Test
    void generatesUsableKeys() {
        String key = SensitiveDataEncryption.newBase64Key();

        assertThat(Base64.getDecoder().decode(key)).hasSize(32);
        var own = new SensitiveDataEncryption(key);
        assertThat(own.decrypt(own.encrypt(Type.PHONE, "+90 555 123 45 67"))).isEqualTo("+90 555 123 45 67");
    }

    @Test
    void rejectsMissingOrWrongSizeKeys() {
        assertThatThrownBy(() -> new SensitiveDataEncryption(null)).hasMessageContaining("not set");
        assertThatThrownBy(() -> new SensitiveDataEncryption("not base64!")).hasMessageContaining("base64");
        assertThatThrownBy(() -> new SensitiveDataEncryption(Base64.getEncoder().encodeToString(new byte[16])))
                .hasMessageContaining("32 bytes");
    }
}
