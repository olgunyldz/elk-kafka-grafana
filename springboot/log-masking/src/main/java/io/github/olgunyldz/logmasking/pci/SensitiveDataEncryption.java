package io.github.olgunyldz.logmasking.pci;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * AES-256-GCM encryption of single values. An encrypted value becomes a typed token:
 *
 * <pre>&lt;type&gt;:v1:&lt;hint&gt;:&lt;base64url(12-byte IV | ciphertext | 16-byte tag)&gt;</pre>
 *
 * <table>
 *   <tr><th>Data</th><th>Token</th><th>Readable hint</th></tr>
 *   <tr><td>card number</td><td>{@code pan:v1:1111:...}</td><td>last 4 digits (allowed by PCI DSS)</td></tr>
 *   <tr><td>IBAN</td><td>{@code iban:v1:TR1326:...}</td><td>country + last 4</td></tr>
 *   <tr><td>national ID</td><td>{@code tckn:v1:46:...}</td><td>last 2 digits</td></tr>
 *   <tr><td>card expiry</td><td>{@code exp:v1::...}</td><td>none</td></tr>
 *   <tr><td>email</td><td>{@code email:v1:example.com:...}</td><td>domain</td></tr>
 *   <tr><td>phone</td><td>{@code phone:v1:67:...}</td><td>last 2 digits</td></tr>
 *   <tr><td>other personal data</td><td>{@code pii:v1::...}</td><td>none</td></tr>
 * </table>
 *
 * The hint lets support staff and searches tell values apart without decrypting. The type and hint are
 * authenticated data: changing either, or moving a payload to another token, makes decryption fail.
 * A random IV per value means the same value never gives the same token twice. Thread-safe.
 */
public class SensitiveDataEncryption {

    /** Kind of data; {@link #token} is the token prefix. */
    public enum Type {
        PAN("pan", "card number"),
        IBAN("iban", "IBAN"),
        TCKN("tckn", "national ID"),
        EXPIRY("exp", "card expiry"),
        EMAIL("email", "email"),
        PHONE("phone", "phone"),
        PERSONAL("pii", "personal data");

        public final String token;
        public final String label;

        Type(String token, String label) {
            this.token = token;
            this.label = label;
        }
    }

    /** Finds tokens inside any text. Group 1: type, group 2: hint, group 3: payload. */
    public static final Pattern TOKEN = Pattern.compile(
            "\\b(pan|iban|tckn|exp|email|phone|pii):v1:([A-Za-z0-9.-]*):([A-Za-z0-9_-]{38,})");

    private static final int IV_BYTES = 12;
    private static final int TAG_BITS = 128;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final SecretKeySpec key;

    /** @param base64Key a standard base64-encoded 32-byte (256-bit) key */
    public SensitiveDataEncryption(String base64Key) {
        if (base64Key == null || base64Key.isBlank()) {
            throw new IllegalStateException("pci.data-encryption-key is not set");
        }
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(base64Key.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("pci.data-encryption-key is not valid base64", e);
        }
        if (bytes.length != 32) {
            throw new IllegalStateException("pci.data-encryption-key must be 32 bytes (256 bits), got " + bytes.length);
        }
        this.key = new SecretKeySpec(bytes, "AES");
    }

    /** A new random key, base64-encoded, for {@code pci.data-encryption-key}. */
    public static String newBase64Key() {
        byte[] k = new byte[32];
        RANDOM.nextBytes(k);
        return Base64.getEncoder().encodeToString(k);
    }

    public String encrypt(Type type, String plaintext) {
        String header = type.token + ":v1:" + hint(type, plaintext);
        try {
            byte[] iv = new byte[IV_BYTES];
            RANDOM.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            cipher.updateAAD(header.getBytes(StandardCharsets.UTF_8));
            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            byte[] out = ByteBuffer.allocate(iv.length + ciphertext.length).put(iv).put(ciphertext).array();
            return header + ":" + Base64.getUrlEncoder().withoutPadding().encodeToString(out);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("Could not encrypt " + type.token, e);
        }
    }

    /** @throws IllegalArgumentException if the token is malformed, tampered with, or from another key */
    public String decrypt(String token) {
        Matcher m = TOKEN.matcher(token == null ? "" : token);
        if (!m.matches()) {
            throw new IllegalArgumentException("Not an encrypted value");
        }
        return decrypt(m.group(1), m.group(2), m.group(3));
    }

    public static boolean isEncrypted(String value) {
        return value != null && TOKEN.matcher(value).matches();
    }

    /** True if the text contains at least one token. */
    public static boolean containsToken(String text) {
        return text != null && text.contains(":v1:") && TOKEN.matcher(text).find();
    }

    /**
     * Decrypts every token inside a text.
     *
     * @throws IllegalArgumentException if any token can't be decrypted (wrong key or modified data):
     *                                  never returns a half-decrypted guess
     */
    public String decryptTokens(String text) {
        if (!containsToken(text)) return text;
        return TOKEN.matcher(text).replaceAll(r -> Matcher.quoteReplacement(decrypt(r.group(1), r.group(2), r.group(3))));
    }

    private String decrypt(String type, String hint, String payload) {
        String header = type + ":v1:" + hint;
        try {
            byte[] in = Base64.getUrlDecoder().decode(payload);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, in, 0, IV_BYTES));
            cipher.updateAAD(header.getBytes(StandardCharsets.UTF_8));
            return new String(cipher.doFinal(in, IV_BYTES, in.length - IV_BYTES), StandardCharsets.UTF_8);
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Cannot decrypt " + header + " (wrong key or modified data)", e);
        }
    }

    /** The readable part of a token: letters, digits, dots and dashes only. Empty when the value is too short. */
    static String hint(Type type, String value) {
        String compact = value.replaceAll("[^A-Za-z0-9]", "");
        return switch (type) {
            case PAN -> compact.length() >= 13 ? compact.substring(compact.length() - 4) : "";
            case IBAN -> compact.length() >= 15
                    ? compact.substring(0, 2).toUpperCase() + compact.substring(compact.length() - 4) : "";
            case TCKN, PHONE -> compact.length() >= 7 ? compact.substring(compact.length() - 2) : "";
            case EMAIL -> value.contains("@")
                    ? value.substring(value.lastIndexOf('@') + 1).replaceAll("[^A-Za-z0-9.-]", "") : "";
            case EXPIRY, PERSONAL -> "";
        };
    }
}
