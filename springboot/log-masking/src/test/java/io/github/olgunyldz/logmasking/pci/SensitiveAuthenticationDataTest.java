package io.github.olgunyldz.logmasking.pci;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class SensitiveAuthenticationDataTest {

    @ParameterizedTest
    @ValueSource(strings = {"cvv", "CVV2", "cvc", "security_code", "securityCode", "pin", "pin-block", "track2", "trackData"})
    void recognizesCardSecurityFieldNames(String field) {
        assertThat(SensitiveAuthenticationData.isSensitiveField(field)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"password", "PWD", "client_secret", "apiKey", "api-key", "accessToken", "refresh_token", "token", "Authorization"})
    void recognizesCredentialFieldNames(String field) {
        assertThat(SensitiveAuthenticationData.isCredentialField(field)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"amount", "cardNumber", "pinned", "service", "trackingId", "tokenCount"})
    void leavesOtherFieldsAlone(String field) {
        assertThat(SensitiveAuthenticationData.isSensitiveField(field)).isFalse();
        assertThat(SensitiveAuthenticationData.isCredentialField(field)).isFalse();
    }

    @Test
    void removesCvvAndPinValuesButKeepsTheirKeys() {
        var result = SensitiveAuthenticationData.removeFromText("Charge failed, CVV: 123, cvc=999 and pin 4321 were sent");

        assertThat(result.text()).isEqualTo("Charge failed, CVV: [REMOVED], cvc=[REMOVED] and pin [REMOVED] were sent");
        assertThat(result.removed()).containsExactly("CVV", "PIN");
    }

    @Test
    void removesCredentialsIncludingBearerTokens() {
        var result = SensitiveAuthenticationData.removeFromText(
                "login password=S3cret! apiKey: sk_live_51Hx9 Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abc done");

        assertThat(result.text()).isEqualTo(
                "login password=[REMOVED] apiKey: [REMOVED] Authorization: [REMOVED] done");
        assertThat(result.removed()).containsExactly("credential");
    }

    @Test
    void keepsTheFullStopAfterAValue() {
        assertThat(SensitiveAuthenticationData.removeFromText("Amex cvv2: 7788. Next").text())
                .isEqualTo("Amex cvv2: [REMOVED]. Next");
        assertThat(SensitiveAuthenticationData.removeFromText("token: abc.def. Next").text())
                .isEqualTo("token: [REMOVED]. Next");
    }

    @Test
    void keepsJsonValidWhenTheValueWasUnquoted() {
        assertThat(SensitiveAuthenticationData.removeFromText("{\"cvv\":123,\"amount\":5}").text())
                .isEqualTo("{\"cvv\":\"[REMOVED]\",\"amount\":5}");
        assertThat(SensitiveAuthenticationData.removeFromText("{\"cvv\":\"123\"}").text())
                .isEqualTo("{\"cvv\":\"[REMOVED]\"}");
    }

    @Test
    void removesTrackData() {
        var track1 = "%B4111111111111111^DOE/JOHN^2512101000000000000?";
        var track2 = ";4111111111111111=25121010000000000?";

        assertThat(SensitiveAuthenticationData.removeFromText("swipe " + track1).text()).isEqualTo("swipe [REMOVED]");
        assertThat(SensitiveAuthenticationData.removeFromText("swipe " + track2).text()).isEqualTo("swipe [REMOVED]");
    }

    @Test
    void leavesOrdinaryTextUnchanged() {
        var result = SensitiveAuthenticationData.removeFromText("Order 42 shipped in 3 boxes, tokens left: 7");

        assertThat(result.text()).isEqualTo("Order 42 shipped in 3 boxes, tokens left: 7");
        assertThat(result.removed()).isEmpty();
    }
}
