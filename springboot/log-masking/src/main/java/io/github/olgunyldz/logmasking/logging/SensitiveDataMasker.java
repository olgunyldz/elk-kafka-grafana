package io.github.olgunyldz.logmasking.logging;

import io.github.olgunyldz.logmasking.pci.CardNumbers;
import io.github.olgunyldz.logmasking.pci.Checksums;
import io.github.olgunyldz.logmasking.pci.SensitiveDataDetector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Masks card and personal data in free text, such as a log message. Unlike Kafka messages, logs are
 * masked, not encrypted: a log reader never needs the real value, and nobody should need a key to read logs.
 * <p>
 * Rules, applied in this order (each can be turned off by name):
 * <ul>
 *   <li>{@code secrets}: values of sensitive keys, e.g. {@code password=...}, {@code "cvv":"123"},
 *       {@code Authorization: Bearer ...} become {@code ***}</li>
 *   <li>{@code personal}: values of personal keys, e.g. {@code holderName=Jane Doe}, {@code "birthDate":"1990-04-12"},
 *       {@code street=Ataturk Cd. 1}. Names and addresses have no pattern of their own, so the key decides;
 *       the value may contain spaces and runs to the next delimiter</li>
 *   <li>{@code expiry}: card expiry dates after a key such as {@code expiry=12/27}</li>
 *   <li>{@code iban}: IBANs with a valid mod-97 checksum; country code and last 4 stay visible</li>
 *   <li>{@code card}: card numbers that pass the Luhn check; only the last 4 digits stay visible (PCI DSS 3.4)</li>
 *   <li>{@code tckn}: Turkish national IDs with valid check digits; last 2 digits stay visible</li>
 *   <li>{@code email}: first character and domain stay visible</li>
 *   <li>{@code phone}: international (+...) and Turkish mobile numbers; last 2 digits stay visible</li>
 * </ul>
 * The number rules check checksums, so order ids, timestamps and offsets are left alone. The patterns are
 * shared with {@link SensitiveDataDetector}, so logs and Kafka messages recognise exactly the same data.
 * Instances are immutable and thread-safe.
 */
public final class SensitiveDataMasker {

    public static final List<String> RULE_NAMES =
            List.of("secrets", "personal", "expiry", "iban", "card", "tckn", "email", "phone");

    /** Keys whose values are always masked; "_" also matches "-" and nothing (access_token, access-token, accessToken). */
    public static final List<String> DEFAULT_SENSITIVE_KEYS = List.of(
            "password", "passwd", "pwd", "secret", "client_secret", "token", "access_token", "refresh_token",
            "id_token", "api_key", "apikey", "authorization", "cvv", "cvv2", "cvc", "cvc2", "cid", "security_code",
            "pin", "pin_block");

    /**
     * Keys whose values are personal data without a recognisable pattern: names, birth dates, addresses.
     * The same fields the Kafka protection encrypts (pci.SensitiveDataDetector).
     */
    public static final List<String> DEFAULT_PERSONAL_KEYS = List.of(
            "first_name", "last_name", "full_name", "surname", "customer_name",
            "holder_name", "cardholder", "cardholder_name", "name_on_card",
            "birth_date", "date_of_birth", "dob",
            "address", "street", "street_address", "address_line1", "address_line2", "city", "postal_code", "zip");

    private static final String MASK = "***";

    private record Rule(String name, Pattern pattern, Function<MatchResult, String> replacement) {
        String apply(String text) {
            Matcher m = pattern.matcher(text);
            if (!m.find()) {
                return text;
            }
            m.reset();
            return m.replaceAll(r -> {
                String replaced = replacement.apply(r);
                return Matcher.quoteReplacement(replaced != null ? replaced : r.group());
            });
        }
    }

    private final List<Rule> rules;

    private SensitiveDataMasker(List<Rule> rules) {
        this.rules = List.copyOf(rules);
    }

    /** All rules, default sensitive keys. */
    public static SensitiveDataMasker defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The text with sensitive data masked; null and empty text are returned as they are. */
    public String mask(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String result = text;
        for (Rule rule : rules) {
            result = rule.apply(result);
        }
        return result;
    }

    public List<String> ruleNames() {
        return rules.stream().map(Rule::name).distinct().toList();
    }

    public static final class Builder {
        private final Set<String> disabled = new LinkedHashSet<>();
        private final Set<String> keys = new LinkedHashSet<>(DEFAULT_SENSITIVE_KEYS);
        private final Set<String> personalKeys = new LinkedHashSet<>(DEFAULT_PERSONAL_KEYS);

        /** Turns off rules by name, see {@link #RULE_NAMES}. */
        public Builder disableRules(String... names) {
            for (String name : names) {
                String n = name.trim().toLowerCase(Locale.ROOT);
                if (!n.isEmpty()) {
                    if (!RULE_NAMES.contains(n)) {
                        throw new IllegalArgumentException("Unknown masking rule '" + name + "', known: " + RULE_NAMES);
                    }
                    disabled.add(n);
                }
            }
            return this;
        }

        /** Adds keys whose values are masked by the "secrets" rule, e.g. "otp", "customer_ref". */
        public Builder addSensitiveKeys(String... extraKeys) {
            Arrays.stream(extraKeys).map(String::trim).filter(k -> !k.isEmpty()).forEach(keys::add);
            return this;
        }

        /** Adds keys whose values are masked by the "personal" rule, e.g. "mother_maiden_name". */
        public Builder addPersonalKeys(String... extraKeys) {
            Arrays.stream(extraKeys).map(String::trim).filter(k -> !k.isEmpty()).forEach(personalKeys::add);
            return this;
        }

        public SensitiveDataMasker build() {
            List<Rule> all = new ArrayList<>();
            all.add(secretsRule(keys));
            all.add(personalRule(personalKeys));
            all.add(EXPIRY);
            all.add(IBAN);
            all.add(CARD);
            all.add(TCKN);
            all.add(EMAIL);
            all.add(PHONE_INTERNATIONAL);
            all.add(PHONE_TR_MOBILE);
            all.removeIf(r -> disabled.contains(r.name()));
            return new SensitiveDataMasker(all);
        }
    }

    // ---- Rules ----

    /** key [:=] value, also "key":"value" and Authorization: Bearer xyz. The key and separator stay, the value becomes ***. */
    private static Rule secretsRule(Set<String> keys) {
        Pattern p = Pattern.compile(
                "(?i)([\"']?\\b(?:" + alternation(keys) + ")\\b[\"']?\\s*[:=]\\s*[\"']?)"
                        // a value starting with [ ([REMOVED]) stays; dots inside, but not at the end
                        + "(?:(?:Bearer|Basic)\\s+)?[^\"'\\s,;&}\\[\\]]*[^\"'\\s,;&}\\[\\].]");
        return new Rule("secrets", p, r -> r.group(1) + MASK);
    }

    /**
     * key [:=] value for personal keys. Unlike secrets, the value may contain spaces ("Jane Doe",
     * "Ataturk Cd. 1"): it runs to the next quote, comma, semicolon, brace, bracket or line end.
     * Already masked or encrypted values (***, [REMOVED], pii:v1:...) and nested objects are left alone.
     */
    private static Rule personalRule(Set<String> keys) {
        Pattern p = Pattern.compile(
                "(?i)([\"']?\\b(?:" + alternation(keys) + ")\\b[\"']?\\s*[:=]\\s*[\"']?)"
                        + "(?![*\\[{]|[a-z]{2,5}:v1:)[^\"',;&{}\\[\\]\\r\\n]*[^\"',;&{}\\[\\]\\r\\n\\s]");
        return new Rule("personal", p, r -> r.group(1) + MASK);
    }

    /** Keys as a regex alternation; "_" also matches "-" and nothing. Longest first, so access_token wins over token. */
    private static String alternation(Set<String> keys) {
        return keys.stream()
                .map(k -> Pattern.quote(k.toLowerCase(Locale.ROOT)).replace("_", "\\E[_-]?\\Q"))
                .sorted((a, b) -> b.length() - a.length())
                .collect(Collectors.joining("|"));
    }

    private static final Rule EXPIRY = new Rule("expiry", SensitiveDataDetector.EXPIRY, r -> r.group(1) + "**/**");

    private static final Rule IBAN = new Rule("iban", SensitiveDataDetector.IBAN,
            r -> Checksums.iban(r.group()) ? keepEnds(r.group(), 2, 4) : null);

    private static final Rule CARD = new Rule("card", CardNumbers.CANDIDATE,
            r -> CardNumbers.isCardNumber(r.group()) ? keepEnds(r.group(), 0, 4) : null);

    private static final Rule TCKN = new Rule("tckn", SensitiveDataDetector.TCKN,
            r -> Checksums.tckn(r.group()) ? keepEnds(r.group(), 0, 2) : null);

    /** The local part of anything@domain, also when the domain is cut off ("john.doe@e..." in a truncated log). */
    private static final Rule EMAIL = new Rule("email", Pattern.compile(
            "(?<![A-Za-z0-9._%+-])([A-Za-z0-9._%+-])[A-Za-z0-9._%+-]*@(?=[A-Za-z0-9-])"),
            r -> r.group(1) + MASK + "@");

    private static final Rule PHONE_INTERNATIONAL = new Rule("phone", SensitiveDataDetector.PHONE_INTERNATIONAL,
            r -> keepEnds(r.group(), 0, 2));

    private static final Rule PHONE_TR_MOBILE = new Rule("phone", SensitiveDataDetector.PHONE_TR_MOBILE,
            r -> keepEnds(r.group(), 0, 2));

    // ---- Helpers ----

    /** Replaces every letter or digit with '*' except the first {@code keepStart} and last {@code keepEnd}; separators stay. */
    static String keepEnds(String value, int keepStart, int keepEnd) {
        int total = (int) value.chars().filter(Character::isLetterOrDigit).count();
        StringBuilder out = new StringBuilder(value.length());
        int seen = 0;
        for (char c : value.toCharArray()) {
            if (Character.isLetterOrDigit(c)) {
                boolean visible = seen < keepStart || seen >= total - keepEnd;
                out.append(visible ? c : '*');
                seen++;
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
