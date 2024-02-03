package ch.hostettler;

import java.util.Random;

final class Rnd extends Random {
    private static final long serialVersionUID = 6025235585395938638L;

    public Rnd() {
        super(System.nanoTime());
    }

    String nextString(int size) {
        if (size <= 16) {
            return Long.toHexString(nextLong());
        }
        if (size <= 32) {
            return Long.toHexString(nextLong()) + Long.toHexString(nextLong());
        }
        if (size <= 64) {
            return Long.toHexString(nextLong()) + Long.toHexString(nextLong()) + Long.toHexString(nextLong());
        }
        return Long.toHexString(nextLong()) + Long.toHexString(nextLong()) + Long.toHexString(nextLong()) + Long.toHexString(nextLong());
    }
}