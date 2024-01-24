package eu.unicore.persist.util;

import java.math.BigInteger;

public class UUID {

	public static String newUniqueID() {
		var u = java.util.UUID.randomUUID();
		BigInteger l = asUnsigned(u.getMostSignificantBits(), u.getLeastSignificantBits());
		return encodeB62(l);
	}

	private static final BigInteger HALF = BigInteger.ONE.shiftLeft(64); // 2^64

	private static BigInteger asUnsigned(long hi, long low) {
		BigInteger l = BigInteger.valueOf(low+(2^64*hi));
		return l.signum() < 0 ? l.add(HALF) : l;
	}

	/**
	 * Base62 encoder from
	 * https://github.com/opencoinage/opencoinage/blob/master/src/java/org/opencoinage/util/Base62.java
	 * @see http://en.wikipedia.org/wiki/Base_62
	 */
	private static final BigInteger BASE = BigInteger.valueOf(62);
	private static final String DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

	/**
	 * Encodes a number using Base62 encoding.
	 *
	 * @param  number a positive integer
	 * @return a Base62 string
	 * @throws IllegalArgumentException if <code>number</code> is a negative integer
	 */
	private static String encodeB62(BigInteger number) {
		if (number.compareTo(BigInteger.ZERO) == -1) { // number < 0
			throw new IllegalArgumentException("number must not be negative");
		}
		StringBuilder result = new StringBuilder();
		while (number.compareTo(BigInteger.ZERO) == 1) { // number > 0
			BigInteger[] divmod = number.divideAndRemainder(BASE);
			number = divmod[0];
			int digit = divmod[1].intValue();
			result.insert(0, DIGITS.charAt(digit));
		}
		return (result.length() == 0) ? DIGITS.substring(0, 1) : result.toString();
	}

}
