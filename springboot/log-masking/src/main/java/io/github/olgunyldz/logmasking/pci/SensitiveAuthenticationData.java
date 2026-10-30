package io.github.olgunyldz.logmasking.pci;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Data that must never be sent or stored, so it is <strong>removed</strong>, never encrypted:
 * <ul>
 *   <li>PCI DSS sensitive authentication data (requirement 3.3): card verification codes, PINs and
 *       magnetic stripe (track) data. It may not be kept after authorization, not even encrypted.</li>
 *   <li>Credentials: passwords, secrets, API keys, tokens and {@code Authorization} headers.</li>
 * </ul>
 * In text, the value is replaced by {@value #REMOVED} and the key stays, so a reader can see that
 * something was taken out: {@code cvc=999} becomes {@code cvc=[REMOVED]}.
 */
public final class SensitiveAuthenticationData {

    /** What removed text is replaced with. */
    public static final String REMOVED = "[REMOVED]";

    /** Field names, compared lower-case without '_', '-' or spaces. */
    private static final Set<String> CARD_FIELDS = Set.of(
            "cvv", "cvv2", "cvc", "cvc2", "cid", "cav2", "cvn",
            "securitycode", "cardsecuritycode", "cardverificationcode", "cardverificationvalue",
            "pin", "pinblock", "pincode",
            "track", "track1", "track2", "trackdata", "magstripe", "magneticstripe");

    private static final Set<String> CREDENTIAL_FIELDS = Set.of(
            "password", "passwd", "pwd", "secret", "clientsecret", "apikey",
            "accesstoken", "refreshtoken", "idtoken", "token", "authorization");

    /**
     * @param group the part of the match that is removed; 0 removes the whole match
     */
    private record TextRule(String label, Pattern pattern, int group) {
    }

    /** A key such as {@code cvv}, {@code "cvc2"} or {@code security_code}, then ':' or '=' (optional for card codes). */
    private static String key(String names, boolean separatorRequired) {
        return "([\"']?\\b(?:" + names + ")\\b[\"']?\\s*" + (separatorRequired ? "[:=]" : "[:=#]?") + "\\s*[\"']?)";
    }

    private static final List<TextRule> TEXT_RULES = List.of(
            // Track 1: %B<PAN>^<NAME>^<YYMM><service code><discretionary data>?
            new TextRule("track data", Pattern.compile("%B\\d{13,19}\\^[^^]{2,26}\\^\\d{4}[^?\\s]*\\?"), 0),
            // Track 2: ;<PAN>=<YYMM><service code><discretionary data>?
            new TextRule("track data", Pattern.compile(";?\\d{13,19}=\\d{4}\\d*\\??"), 0),
            new TextRule("CVV", Pattern.compile("(?i)" + key("cvv2?|cvc2?|cid|cav2|security[ _-]?code", false) + "(\\d{3,4})\\b"), 2),
            new TextRule("PIN", Pattern.compile("(?i)" + key("pin(?:[ _-]?(?:code|block))?", false) + "(\\d{4,12})\\b"), 2),
            // A credential value may contain dots (JWTs, API keys) but doesn't end with one: "token: abc." keeps its full stop
            new TextRule("credential", Pattern.compile("(?i)"
                    + key("password|passwd|pwd|client[_-]?secret|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|token|authorization", true)
                    + "((?:(?:Bearer|Basic)\\s+)?[^\"'\\s,;&}\\[\\]]*[^\"'\\s,;&}\\[\\].])"), 2));

    /** The text with SAD and credentials replaced by {@link #REMOVED}, and what kind of data was removed. */
    public record Removal(String text, List<String> removed) {
    }

    private SensitiveAuthenticationData() {
    }

    /** True for card security data fields: cvv, pin, track2… */
    public static boolean isSensitiveField(String fieldName) {
        return fieldName != null && CARD_FIELDS.contains(normalize(fieldName));
    }

    /** True for credential fields: password, apiKey, accessToken… */
    public static boolean isCredentialField(String fieldName) {
        return fieldName != null && CREDENTIAL_FIELDS.contains(normalize(fieldName));
    }

    public static Removal removeFromText(String text) {
        if (text == null || text.isEmpty()) return new Removal(text, List.of());
        var removed = new ArrayList<String>();
        String result = text;
        for (var rule : TEXT_RULES) {
            Matcher m = rule.pattern().matcher(result);
            if (!m.find()) continue;
            if (!removed.contains(rule.label())) removed.add(rule.label());
            m.reset();
            result = m.replaceAll(match -> Matcher.quoteReplacement(replacement(match.group(), match, rule.group())));
        }
        return new Removal(result, removed);
    }

    private static String replacement(String whole, java.util.regex.MatchResult match, int group) {
        if (group == 0) return REMOVED;
        String keyPart = match.group(1);
        // JSON with an unquoted value ("cvv":123): quote the replacement so the JSON stays valid
        boolean jsonUnquoted = keyPart.startsWith("\"") && !keyPart.endsWith("\"");
        String before = whole.substring(0, match.start(group) - match.start());
        String after = whole.substring(match.end(group) - match.start());
        return before + (jsonUnquoted ? "\"" + REMOVED + "\"" : REMOVED) + after;
    }

    static String normalize(String fieldName) {
        return fieldName.toLowerCase(Locale.ROOT).replaceAll("[_\\-\\s]", "");
    }
}
