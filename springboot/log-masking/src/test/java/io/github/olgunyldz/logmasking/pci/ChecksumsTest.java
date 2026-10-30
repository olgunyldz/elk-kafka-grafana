package io.github.olgunyldz.logmasking.pci;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class ChecksumsTest {

    @ParameterizedTest
    @ValueSource(strings = {"4111111111111111", "5555555555554444", "378282246310005", "79927398713"})
    void luhnAcceptsValidNumbers(String digits) {
        assertThat(Checksums.luhn(digits)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"4111111111111112", "1234567890123", "", "4111-1111"})
    void luhnRejectsInvalidNumbers(String digits) {
        assertThat(Checksums.luhn(digits)).isFalse();
    }

    @Test
    void luhnRejectsNull() {
        assertThat(Checksums.luhn(null)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"GB82WEST12345698765432", "TR330006100519786457841326", "tr33 0006 1005 1978 6457 8413 26"})
    void ibanAcceptsValidIbans(String iban) {
        assertThat(Checksums.iban(iban)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"GB82WEST12345698765433", "TR33000610051978645784132", "NOTANIBAN"})
    void ibanRejectsInvalidIbans(String iban) {
        assertThat(Checksums.iban(iban)).isFalse();
    }

    @Test
    void tcknChecksBothCheckDigits() {
        assertThat(Checksums.tckn("10000000146")).isTrue();
        assertThat(Checksums.tckn("10000000147")).isFalse();
        assertThat(Checksums.tckn("10000000156")).isFalse();
        // Must be 11 digits and must not start with 0
        assertThat(Checksums.tckn("01000000146")).isFalse();
        assertThat(Checksums.tckn("1000000014")).isFalse();
    }
}
