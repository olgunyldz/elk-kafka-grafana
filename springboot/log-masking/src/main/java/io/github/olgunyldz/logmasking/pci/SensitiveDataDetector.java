package io.github.olgunyldz.logmasking.pci;

import io.github.olgunyldz.logmasking.pci.SensitiveDataEncryption.Type;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;
import tools.jackson.databind.node.StringNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Walks a JSON message and protects every sensitive part of it:
 * <ul>
 *   <li><b>Removed:</b> card security data (CVV, PIN, track data) and credentials (passwords, API keys,
 *       tokens), as fields or inside text.</li>
 *   <li><b>Encrypted:</b> card numbers, card expiry dates and personal data (names, emails, phones,
 *       addresses, national IDs, IBANs…), as whole fields or inside text.</li>
 * </ul>
 * Everything else is left unchanged. The input node is not modified; {@link #protect} returns a copy.
 */
public class SensitiveDataDetector {

    public enum Action { REMOVED, ENCRYPTED }

    /** One protected part of a message, for example {@code payload.card.cvv} removed as "CVV". */
    public record Finding(String path, Action action, String what) {
        public String describe() {
            return path + " (" + what + ")";
        }
    }

    public record Result(JsonNode node, List<Finding> findings) {
        public List<String> removed() {
            return findings.stream().filter(f -> f.action() == Action.REMOVED).map(Finding::describe).toList();
        }

        public List<String> encrypted() {
            return findings.stream().filter(f -> f.action() == Action.ENCRYPTED).map(Finding::describe).toList();
        }
    }

    /**
     * Field names whose value is always sensitive, and the token type it is encrypted as.
     * Compared lower-case without '_', '-' or spaces.
     */
    private static final Map<String, Type> FIELD_TYPES = Map.ofEntries(
            // card data
            Map.entry("cardnumber", Type.PAN), Map.entry("pan", Type.PAN), Map.entry("primaryaccountnumber", Type.PAN),
            Map.entry("ccnumber", Type.PAN), Map.entry("creditcardnumber", Type.PAN), Map.entry("cardno", Type.PAN),
            Map.entry("expiry", Type.EXPIRY), Map.entry("expirydate", Type.EXPIRY), Map.entry("exp", Type.EXPIRY),
            Map.entry("expdate", Type.EXPIRY), Map.entry("expiration", Type.EXPIRY), Map.entry("expirationdate", Type.EXPIRY),
            Map.entry("cardexpiry", Type.EXPIRY), Map.entry("cardexpirydate", Type.EXPIRY),
            Map.entry("cardexpirationdate", Type.EXPIRY), Map.entry("validthru", Type.EXPIRY),
            // contact
            Map.entry("email", Type.EMAIL), Map.entry("emailaddress", Type.EMAIL),
            Map.entry("phone", Type.PHONE), Map.entry("phonenumber", Type.PHONE), Map.entry("mobile", Type.PHONE),
            Map.entry("mobilephone", Type.PHONE), Map.entry("gsm", Type.PHONE),
            // identity and bank
            Map.entry("nationalid", Type.TCKN), Map.entry("tckn", Type.TCKN), Map.entry("tcno", Type.TCKN),
            Map.entry("tckimlikno", Type.TCKN), Map.entry("identitynumber", Type.TCKN),
            Map.entry("ssn", Type.PERSONAL), Map.entry("passportnumber", Type.PERSONAL),
            Map.entry("iban", Type.IBAN),
            // names, address, birth date
            Map.entry("firstname", Type.PERSONAL), Map.entry("lastname", Type.PERSONAL), Map.entry("fullname", Type.PERSONAL),
            Map.entry("surname", Type.PERSONAL), Map.entry("customername", Type.PERSONAL),
            Map.entry("cardholder", Type.PERSONAL), Map.entry("cardholdername", Type.PERSONAL),
            Map.entry("holdername", Type.PERSONAL), Map.entry("nameoncard", Type.PERSONAL),
            Map.entry("address", Type.PERSONAL), Map.entry("streetaddress", Type.PERSONAL), Map.entry("street", Type.PERSONAL),
            Map.entry("addressline1", Type.PERSONAL), Map.entry("addressline2", Type.PERSONAL),
            Map.entry("birthdate", Type.PERSONAL), Map.entry("dateofbirth", Type.PERSONAL), Map.entry("dob", Type.PERSONAL));

    static final Pattern IBAN = Pattern.compile("\\b[A-Z]{2}\\d{2}(?: ?[A-Z0-9]{4}){2,7}(?: ?[A-Z0-9]{1,4})?\\b");
    static final Pattern TCKN = Pattern.compile("(?<!\\d)[1-9]\\d{10}(?!\\d)");
    /** Group 2 is the date: expiry=12/27, "expDate":"2027-12" */
    static final Pattern EXPIRY = Pattern.compile(
            "(?i)([\"']?\\b(?:exp(?:iry|iration)?(?:[_-]?date)?|card[_-]?exp(?:iry|iration)?(?:[_-]?date)?|valid[_ -]?thru)\\b[\"']?\\s*[:=]?\\s*[\"']?)"
                    + "(\\d{1,2}\\s*/\\s*\\d{2,4}|\\d{4}-\\d{2}(?:-\\d{2})?)");
    static final Pattern EMAIL = Pattern.compile(
            "(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\\.[A-Za-z0-9-]+)*\\.[A-Za-z]{2,}\\b");
    static final Pattern PHONE_INTERNATIONAL = Pattern.compile("(?<![\\w+])\\+\\d(?:[ ().-]?\\d){7,14}(?!\\d)");
    /** Turkish mobile: 5xx xxx xx xx, optionally with a leading 0. */
    static final Pattern PHONE_TR_MOBILE = Pattern.compile("(?<![\\d*])0?5\\d{2}(?:[ -]?\\d){7}(?!\\d)");

    /** A pattern for data inside free text; only {@code group} is encrypted. */
    private record TextDetector(Type type, Pattern pattern, int group, Predicate<String> valid) {
    }

    /** Earlier detectors win: an IBAN's digits are encrypted as the IBAN, not again as something else. */
    private static final List<TextDetector> TEXT_DETECTORS = List.of(
            new TextDetector(Type.IBAN, IBAN, 0, Checksums::iban),
            new TextDetector(Type.PAN, CardNumbers.CANDIDATE, 0, CardNumbers::isCardNumber),
            new TextDetector(Type.TCKN, TCKN, 0, Checksums::tckn),
            new TextDetector(Type.EXPIRY, EXPIRY, 2, v -> true),
            new TextDetector(Type.EMAIL, EMAIL, 0, v -> true),
            new TextDetector(Type.PHONE, PHONE_INTERNATIONAL, 0, v -> true),
            new TextDetector(Type.PHONE, PHONE_TR_MOBILE, 0, v -> true));

    private final SensitiveDataEncryption encryption;

    public SensitiveDataDetector(SensitiveDataEncryption encryption) {
        this.encryption = encryption;
    }

    public Result protect(JsonNode root) {
        var findings = new ArrayList<Finding>();
        JsonNode copy = root.deepCopy();
        if (copy.isString()) {
            copy = StringNode.valueOf(protectText(copy.asString(), "$", findings));
        } else {
            walk(copy, "", null, findings);
        }
        return new Result(copy, List.copyOf(findings));
    }

    /** What sensitive data a plain text contains, without changing it. Used to check Kafka keys. */
    public List<String> findInText(String text) {
        var findings = new ArrayList<Finding>();
        protectText(text, "$", findings);
        return findings.stream().map(Finding::what).distinct().toList();
    }

    // ---- JSON tree ----

    /** @param inherited the type of an enclosing sensitive field (e.g. everything under "address"), or null */
    private void walk(JsonNode node, String path, Type inherited, List<Finding> findings) {
        if (node instanceof ObjectNode object) {
            for (String name : List.copyOf(object.propertyNames())) {
                String childPath = path.isEmpty() ? name : path + "." + name;
                String normalized = SensitiveAuthenticationData.normalize(name);
                JsonNode child = object.get(name);
                Type fieldType = FIELD_TYPES.get(normalized);

                if (SensitiveAuthenticationData.isSensitiveField(name)) {
                    object.remove(name);
                    findings.add(new Finding(childPath, Action.REMOVED, sadLabel(normalized)));
                } else if (SensitiveAuthenticationData.isCredentialField(name)) {
                    object.remove(name);
                    findings.add(new Finding(childPath, Action.REMOVED, "credential"));
                } else if (child.isContainer()) {
                    walk(child, childPath, fieldType != null ? Type.PERSONAL : inherited, findings);
                } else if ((fieldType != null || inherited != null) && isScalar(child)) {
                    Type type = fieldType != null ? fieldType : inherited;
                    object.put(name, encryption.encrypt(type, scalarText(child)));
                    findings.add(new Finding(childPath, Action.ENCRYPTED, type.label));
                } else {
                    JsonNode replaced = protectScalar(child, childPath, findings);
                    if (replaced != child) object.set(name, replaced);
                }
            }
        } else if (node instanceof ArrayNode array) {
            for (int i = 0; i < array.size(); i++) {
                JsonNode child = array.get(i);
                String childPath = path + "[" + i + "]";
                if (child.isContainer()) {
                    walk(child, childPath, inherited, findings);
                } else if (inherited != null && isScalar(child)) {
                    array.set(i, StringNode.valueOf(encryption.encrypt(inherited, scalarText(child))));
                    findings.add(new Finding(childPath, Action.ENCRYPTED, inherited.label));
                } else {
                    array.set(i, protectScalar(child, childPath, findings));
                }
            }
        }
    }

    /** A value in a field with an ordinary name: check what the value itself contains. */
    private JsonNode protectScalar(JsonNode node, String path, List<Finding> findings) {
        if (node.isString()) {
            String text = node.asString();
            String result = protectText(text, path, findings);
            return result.equals(text) ? node : StringNode.valueOf(result);
        }
        if (node.isIntegralNumber()) {
            String digits = node.bigIntegerValue().toString();
            Type type = CardNumbers.isCardNumber(digits) ? Type.PAN : Checksums.tckn(digits) ? Type.TCKN : null;
            if (type != null) {
                findings.add(new Finding(path, Action.ENCRYPTED, type.label));
                return StringNode.valueOf(encryption.encrypt(type, digits));
            }
        }
        return node;
    }

    // ---- Free text ----

    String protectText(String text, String path, List<Finding> findings) {
        if (text == null || text.isEmpty() || SensitiveDataEncryption.isEncrypted(text)) return text;

        var removal = SensitiveAuthenticationData.removeFromText(text);
        removal.removed().forEach(what -> findings.add(new Finding(path, Action.REMOVED, what)));
        String result = removal.text();

        if (CardNumbers.isCardNumber(result.strip())) {
            findings.add(new Finding(path, Action.ENCRYPTED, Type.PAN.label));
            return encryption.encrypt(Type.PAN, result.strip());
        }
        for (var detector : TEXT_DETECTORS) {
            result = encryptMatches(result, detector, path, findings);
        }
        return result;
    }

    /** Encrypts each accepted match in place, skipping text that is already a token. */
    private String encryptMatches(String text, TextDetector detector, String path, List<Finding> findings) {
        var out = new StringBuilder();
        int last = 0;
        boolean found = false;
        Matcher tokens = SensitiveDataEncryption.TOKEN.matcher(text);
        while (true) {
            boolean hasToken = tokens.find();
            String segment = text.substring(last, hasToken ? tokens.start() : text.length());
            Matcher m = detector.pattern().matcher(segment);
            int pos = 0;
            while (m.find()) {
                String value = m.group(detector.group());
                if (!detector.valid().test(value)) continue;
                found = true;
                out.append(segment, pos, m.start(detector.group())).append(encryption.encrypt(detector.type(), value));
                pos = m.end(detector.group());
            }
            out.append(segment, pos, segment.length());
            if (!hasToken) break;
            out.append(tokens.group());
            last = tokens.end();
        }
        var finding = new Finding(path, Action.ENCRYPTED, detector.type().label);
        // Two phone patterns share one label: report it once
        if (found && !findings.contains(finding)) findings.add(finding);
        return out.toString();
    }

    // ---- Helpers ----

    private static boolean isScalar(JsonNode node) {
        return node != null && node.isValueNode() && !node.isNull();
    }

    private static String scalarText(JsonNode node) {
        return node.isString() ? node.asString() : node.toString();
    }

    private static String sadLabel(String normalizedField) {
        if (normalizedField.startsWith("pin")) return "PIN";
        if (normalizedField.startsWith("track") || normalizedField.contains("stripe")) return "track data";
        return "CVV";
    }
}
