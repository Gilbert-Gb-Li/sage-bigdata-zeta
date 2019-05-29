package com.haima.sage.bigdata.analyzer.aggregation.math;

//import com.google.common.hash.HashCode;
//import com.google.common.hash.HashFunction;
//import com.google.common.hash.Hashing;
//import com.google.common.io.BaseEncoding;

import java.util.List;


/**
 * This class is MinHash utility.
 *
 * @author shinsuke
 *
 */
public class MinHash {

//    private HashFunction[] hashFunctions;

    private int hashBit;

    private long[] minHashValues;

//    private String minHash;
//
//    /**
//     * Create hash functions.
//     *
//     * @param seed a base seed
//     * @param num the number of hash functions.
//     * @return
//     */
//    public static HashFunction[] createHashFunctions(final int seed,
//                                                     final int num) {
//        final HashFunction[] hashFunctions = new HashFunction[num];
//        for (int i = 0; i < num; i++) {
//            hashFunctions[i] = Hashing.murmur3_128(seed + i);
//        }
//        return hashFunctions;
//    }

    public static long[] minhash(List<String> texts) {
        // The number of bits for each hash value.
        int hashBit = 1;
        // A base seed for hash functions.
        int seed = 0;
        // The number of hash functions.
        int num = 64;

        return minhash(texts, hashBit, seed, num);
    }

    public static long[] minhash(List<String> texts, final int hashBit, final int seed,
                                 final int num) {
//        final HashFunction[] hashFunctions = MinHash.createHashFunctions(seed, num);

        MinHash minhash = new MinHash(hashBit); /*hashFunctions, */
        return minhash.process(texts);
    }

    public MinHash(/*final HashFunction[] hashFunctions, */final int hashBit) {
//        this.hashFunctions = hashFunctions;
        this.hashBit = hashBit;
        minHashValues = new long[128]; //new long[hashFunctions.length];
    }

    public final long[] process(List<String> texts) {
        final int funcSize = 128; //hashFunctions.length;
        for (String term : texts) {
            for (int i = 0; i < funcSize; i++) {
//                final HashCode hashCode = hashFunctions[i]
//                        .hashUnencodedChars(term);
//                final long value = hashCode.asLong();
                final long value = SpookyHash64.hash(term, i);
                if (value < minHashValues[i]) {
                    minHashValues[i] = value;
                }
            }
        }

        return calcMinHash(minHashValues, hashBit);
    }

    public static int distance(long[] hash1, long[] hash2) {
        if (hash1.length != hash2.length) {
            throw new IllegalArgumentException("unequal length " + hash1.length
                    + " and " + hash2.length);
        }

        int count = 0;

        for (int i = 0; i < hash1.length; i++) {
            long z = hash1[i] ^ hash2[i];
            while (z != 0) {
                count++;
                z &= z - 1;
            }
        }

        return count;
    }

    protected static long[] calcMinHash(final long[] minHashValues,
                                        final int hashBit) {
        final int shift = 1;
        final int radix = 1 << shift;
        final long mask = radix - 1;
        int pos = 0;
        final int nbits = minHashValues.length * hashBit;
        final FastBitSet bitSet = new FastBitSet(nbits);
        for (long i : minHashValues) {
            for (int j = 0; j < hashBit; j++) {
                bitSet.set(pos, (int) (i & mask) == 1);
                pos++;
                i >>>= shift;
            }
        }
        return bitSet.toByteArray();
    }

    /**
     * Returns a string formatted by bits.
     *
     * @param data
     * @return
     */
    public static String toBinaryString(final byte[] data) {
        if (data == null) {
            return null;
        }
        final StringBuilder buf = new StringBuilder(data.length * 8);
        for (final byte element : data) {
            byte bits = element;
            for (int j = 0; j < 8; j++) {
                if ((bits & 0x80) == 0x80) {
                    buf.append('1');
                } else {
                    buf.append('0');
                }
                bits <<= 1;
            }
        }
        return buf.toString();
    }
}
