package io.github.olgunyldz.logmasking.pci;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class CardNumbersTest {

    @ParameterizedTest
    @CsvSource({
            "4111111111111111, Visa",
            "5555555555554444, Mastercard",
            "2223003122003222, Mastercard",
            "378282246310005, American Express",
            "6011111111111117, Discover",
            "3530111333300000, JCB"})
    void recognizesTestCardsAndTheirBrand(String pan, String brand) {
        assertThat(CardNumbers.isCardNumber(pan)).isTrue();
        assertThat(CardNumbers.brand(pan)).contains(brand);
    }

    @Test
    void allowsSpacesAndDashes() {
        assertThat(CardNumbers.isCardNumber("4111 1111 1111 1111")).isTrue();
        assertThat(CardNumbers.isCardNumber("4111-1111-1111-1111")).isTrue();
    }

    @Test
    void rejectsNumbersThatOnlyLookLikeCards() {
        assertThat(CardNumbers.isCardNumber("4111111111111112")).as("fails Luhn").isFalse();
        assertThat(CardNumbers.isCardNumber("1000000000000008")).as("no card network starts with 1").isFalse();
        assertThat(CardNumbers.isCardNumber("411111111111")).as("too short").isFalse();
        assertThat(CardNumbers.isCardNumber(null)).isFalse();
    }

    @Test
    void findsEveryCardNumberInText() {
        var text = "Paid with 4111 1111 1111 1111, refund to 5555-5555-5555-4444; order 1234567890123456";
        var matches = CardNumbers.findAll(text);

        assertThat(matches).extracting(CardNumbers.Match::digits)
                .containsExactly("4111111111111111", "5555555555554444");
        assertThat(text.substring(matches.get(0).start(), matches.get(0).end())).isEqualTo("4111 1111 1111 1111");
    }

    @Test
    void ignoresDigitsInsideLongerNumbers() {
        assertThat(CardNumbers.findAll("id 94111111111111111")).isEmpty();
    }

    @Test
    void masksAllButFirstSixAndLastFour() {
        assertThat(CardNumbers.mask("4111111111111111")).isEqualTo("411111******1111");
        assertThat(CardNumbers.mask("378282246310005")).isEqualTo("378282*****0005");
    }
}
