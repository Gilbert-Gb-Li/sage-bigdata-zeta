package com.haima.sage.bigdata.analyzer.aggregation.math;


/**
 * Immutable bits set.
 *
 * @author shinsuke
 *
 */
public class FastBitSet {
    final long[] data;

    final int nbit;

    public FastBitSet(final int nbit) {
        this.nbit = nbit;
        if (nbit == 0) {
            throw new IllegalArgumentException("nbit is above 0.");
        }

        data = new long[(nbit - 1) / 64 + 1];
    }

    public void set(final int bitIndex, final boolean value) {
        final int bytePos = bitIndex / 64;
        final int bitPos = bitIndex % 64;

        if (bytePos >= data.length) {
            return;
        }

        long mask = 0x0000000000000001L << bitPos;
        long notMask = 0xFFFFFFFFFFFFFFFFL ^ mask;

        data[bytePos] = (data[bytePos] & notMask);
        if (value) {
            data[bytePos] = (data[bytePos] | mask);
        }
    }

    public long[] toByteArray() {
        return data;
    }
}
