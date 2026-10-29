package io.github.olgunyldz.logmasking.pci;

import java.math.BigInteger;

/**
 * Check-digit algorithms. A number that only <em>looks</em> like a card number or an ID
 * (an order id, a timestamp) rarely passes its checksum, so these keep false positives low.
 */
public final class Checksums {

    private Checksums() {
    }

    /** Luhn (mod 10), used by every payment card number. */
    public static boolean luhn(String digits) {
        if (digits == null || digits.isEmpty() || !digits.chars().allMatch(Character::isDigit)) {
            return false;
        }
        int sum = 0;
        boolean doubleIt = false;
        for (int i = digits.length() - 1; i >= 0; i--) {
            int d = digits.charAt(i) - '0';
            if (doubleIt) {
                d *= 2;
                if (d > 9) d -= 9;
            }
            sum += d;
            doubleIt = !doubleIt;
        }
        return sum % 10 == 0;
    }

    /** IBAN (ISO 13616, mod 97). Spaces are ignored, letters are case-insensitive. */
    public static boolean iban(String value) {
        if (value == null) return false;
        String iban = value.replace(" ", "").toUpperCase();
        if (iban.length() < 15 || iban.length() > 34 || !iban.matches("[A-Z]{2}\\d{2}[A-Z0-9]+")) {
            return false;
        }
        String rearranged = iban.substring(4) + iban.substring(0, 4);
        var numeric = new StringBuilder();
        for (char c : rearranged.toCharArray()) {
            numeric.append(Character.isDigit(c) ? String.valueOf(c) : String.valueOf(c - 'A' + 10));
        }
        return new BigInteger(numeric.toString()).mod(BigInteger.valueOf(97)).intValue() == 1;
    }

    /** Turkish national ID number (T.C. Kimlik No): 11 digits, two check digits. */
    public static boolean tckn(String digits) {
        if (digits == null || !digits.matches("[1-9]\\d{10}")) return false;
        int[] d = digits.chars().map(c -> c - '0').toArray();
        int odd = d[0] + d[2] + d[4] + d[6] + d[8];
        int even = d[1] + d[3] + d[5] + d[7];
        int tenth = Math.floorMod(odd * 7 - even, 10);
        int eleventh = (odd + even + d[9]) % 10;
        return d[9] == tenth && d[10] == eleventh;
    }
}
