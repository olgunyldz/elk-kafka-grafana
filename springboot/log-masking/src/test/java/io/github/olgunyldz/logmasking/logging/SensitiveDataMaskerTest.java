package io.github.olgunyldz.logmasking.logging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SensitiveDataMaskerTest {

    private final SensitiveDataMasker masker = SensitiveDataMasker.defaults();

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', textBlock = """
            # ---- card numbers (Luhn-valid), only the last 4 stay
            Visa                    | card 4111111111111111 ok        | card ************1111 ok
            Visa with spaces        | 4111 1111 1111 1111             | **** **** **** 1111
            Mastercard with dashes  | 5555-5555-5555-4444             | ****-****-****-4444
            Amex 15 digits          | amex=378282246310005            | amex=***********0005
            # ---- secrets by key
            password=               | login password=hunter2 user=bob | login password=*** user=bob
            JSON cvv                | {"cvv":"123","amount":10}       | {"cvv":"***","amount":10}
            CVC with colon          | cvc: 999                        | cvc: ***
            Bearer token            | Authorization: Bearer abc.def.g | Authorization: ***
            camelCase key           | accessToken=xyz123              | accessToken=***
            PIN                     | pin=1234                        | pin=***
            expiry                  | expiry=12/27                    | expiry=**/**
            # ---- personal keys: the whole value, spaces included, up to the next delimiter
            name in a Map           | card={holderName=Jane Doe, expiry=**/**} | card={holderName=***, expiry=**/**}
            JSON first name         | {"firstName":"Jane","amount":5} | {"firstName":"***","amount":5}
            street with dots        | address={city=Istanbul, street=Ataturk Cd. 1} | address={city=***, street=***}
            birth date              | birthDate=1990-04-12, next     | birthDate=***, next
            no delimiter: masks on  | birthDate=1990-04-12 next      | birthDate=***
            # ---- IBAN (mod-97 valid), country code and last 4 stay
            IBAN TR                 | iban TR330006100519786457841326 | iban TR********************1326
            IBAN GB with spaces     | GB82 WEST 1234 5698 7654 32     | GB** **** **** **** **54 32
            # ---- Turkish national ID (checksum valid), last 2 stay
            TCKN                    | tckn 10000000146                | tckn *********46
            # ---- email and phone
            email                   | mail john.doe@example.com now   | mail j***@example.com now
            email cut off           | payload='mail john.doe@e...'    | payload='mail j***@e...'
            international phone     | call +90 532 123 45 67          | call +** *** *** ** 67
            Turkish mobile          | gsm 0532 123 45 67              | gsm **** *** ** 67
            """)
    void masks(String name, String input, String expected) {
        assertThat(masker.mask(input)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', textBlock = """
            Luhn-invalid 16 digits  | order 4111111111111112
            epoch millis            | Timestamp: 1793271793911
            Kafka log line          | Key: order-42 Value: OrderPlaced Partition: 2 Offset: 41
            invalid TCKN            | ref 12345678901
            invalid IBAN            | TR330006100519786457841327
            no separator after key  | password reset failed for user bob
            plain sentence          | Started ConsumerApplication in 3.5 seconds
            message removed in step 03 | card cvc=[REMOVED] pin=[REMOVED]
            encrypted personal value | holderName=pii:v1::RdFe9ymNyeF9lXCS6wc-dzjgWw1q9mHXHPWv-8sF0QE
            name of a metric        | name=cpu_usage_percent value=42
            """)
    void leavesAlone(String name, String input) {
        assertThat(masker.mask(input)).isEqualTo(input);
    }

    @Test
    void keepsTheReadableHintsOfEncryptedTokens() {
        // Kafka tokens from step 03 are already safe; their hints stay readable in logs
        String token = "pan:v1:1111:awbnPFOL85sIFVZlMXRtTgaWBaE82DkpvKkUPhL5-uqCAi_Z4Ox-nCzmceVnM1A";
        assertThat(masker.mask("sent " + token)).isEqualTo("sent " + token);
    }

    @Test
    void nullAndEmpty() {
        assertThat(masker.mask(null)).isNull();
        assertThat(masker.mask("")).isEmpty();
    }

    @Test
    void rulesCanBeDisabled() {
        SensitiveDataMasker noEmail = SensitiveDataMasker.builder().disableRules("email", " phone ").build();

        assertThat(noEmail.mask("john.doe@example.com +90 532 123 45 67")).isEqualTo("john.doe@example.com +90 532 123 45 67");
        assertThat(noEmail.mask("4111111111111111")).isEqualTo("************1111");
        assertThat(noEmail.ruleNames()).doesNotContain("email", "phone");
    }

    @Test
    void extraKeys() {
        SensitiveDataMasker withOtp = SensitiveDataMasker.builder().addSensitiveKeys("otp", "customer_ref").build();

        assertThat(withOtp.mask("otp=482913 customerRef=C-77")).isEqualTo("otp=*** customerRef=***");
    }

    @Test
    void extraPersonalKeys() {
        SensitiveDataMasker withMaiden = SensitiveDataMasker.builder().addPersonalKeys("mother_maiden_name").build();

        assertThat(withMaiden.mask("motherMaidenName=Ayse Kaya, ok")).isEqualTo("motherMaidenName=***, ok");
    }

    @Test
    void unknownRuleIsAnError() {
        assertThatThrownBy(() -> SensitiveDataMasker.builder().disableRules("cards"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cards");
    }
}
