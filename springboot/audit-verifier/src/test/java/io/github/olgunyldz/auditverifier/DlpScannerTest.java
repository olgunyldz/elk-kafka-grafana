package io.github.olgunyldz.auditverifier;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DlpScannerTest {

    @Test
    void findsCardNumbers() {
        assertThat(DlpScanner.find("{\"message\":\"Payment for card 4111111111111111 approved\"}")).isEqualTo("card number");
        assertThat(DlpScanner.find("card 4111 1111 1111 1111")).isEqualTo("card number");
    }

    @Test
    void findsNationalIds() {
        assertThat(DlpScanner.find("customer 10000000146 logged in")).isEqualTo("national ID");
    }

    @Test
    void ignoresMaskedValuesAndLookalikes() {
        assertThat(DlpScanner.find("card 411111******1111")).isNull();
        // 16 digits without a valid Luhn check, 11 digits without valid national ID check digits
        assertThat(DlpScanner.find("order 4111111111111112, ref 12345678901")).isNull();
    }

    @Test
    void ignoresEncryptedTokens() {
        assertThat(DlpScanner.find("pan:v1:1111:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4v")).isNull();
    }
}
