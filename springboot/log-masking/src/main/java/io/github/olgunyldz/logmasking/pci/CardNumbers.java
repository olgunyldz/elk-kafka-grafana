package io.github.olgunyldz.logmasking.pci;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Finds payment card numbers (PANs). A candidate must have 13–19 digits (spaces or dashes
 * between them are allowed), start with a known card network prefix, and pass the Luhn check.
 */
public final class CardNumbers {

    /** 13–19 digits, optionally separated by single spaces or dashes, not part of a longer number. */
    public static final Pattern CANDIDATE = Pattern.compile("(?<![\\d-])\\d(?:[ -]?\\d){12,18}(?![\\d-])");

    /** A match in a text: where it is, and the digits without separators. */
    public record Match(int start, int end, String digits) {
    }

    private CardNumbers() {
    }

    /** True when the value, ignoring spaces and dashes, is a valid card number. */
    public static boolean isCardNumber(String value) {
        if (value == null) return false;
        String digits = value.replaceAll("[ -]", "");
        return digits.length() >= 13 && digits.length() <= 19
                && digits.chars().allMatch(Character::isDigit)
                && brand(digits).isPresent()
                && Checksums.luhn(digits);
    }

    /** Every card number inside a text, in order. */
    public static List<Match> findAll(String text) {
        var matches = new ArrayList<Match>();
        if (text == null) return matches;
        var m = CANDIDATE.matcher(text);
        while (m.find()) {
            if (isCardNumber(m.group())) {
                matches.add(new Match(m.start(), m.end(), m.group().replaceAll("[ -]", "")));
            }
        }
        return matches;
    }

    /** The card network, from the first digits (the IIN). */
    public static Optional<String> brand(String digits) {
        if (digits == null || digits.length() < 6) return Optional.empty();
        int p1 = digits.charAt(0) - '0';
        int p2 = Integer.parseInt(digits.substring(0, 2));
        int p4 = Integer.parseInt(digits.substring(0, 4));
        if (p1 == 4) return Optional.of("Visa");
        if ((p2 >= 51 && p2 <= 55) || (p4 >= 2221 && p4 <= 2720)) return Optional.of("Mastercard");
        if (p2 == 34 || p2 == 37) return Optional.of("American Express");
        if (p4 == 9792) return Optional.of("Troy");
        if (p4 >= 3528 && p4 <= 3589) return Optional.of("JCB");
        if (p2 == 36 || p2 == 38 || (p4 >= 3000 && p4 <= 3059)) return Optional.of("Diners Club");
        if (p4 == 6011 || p2 == 65 || (p4 >= 6440 && p4 <= 6499)) return Optional.of("Discover");
        if (p2 == 62) return Optional.of("UnionPay");
        if (p2 == 50 || (p2 >= 56 && p2 <= 69)) return Optional.of("Maestro");
        return Optional.empty();
    }

    /** PCI DSS allows showing at most the first 6 and last 4 digits: 411111******1111. */
    public static String mask(String digits) {
        if (digits == null || digits.length() < 13) return "****";
        return digits.substring(0, 6) + "*".repeat(digits.length() - 10) + digits.substring(digits.length() - 4);
    }
}
