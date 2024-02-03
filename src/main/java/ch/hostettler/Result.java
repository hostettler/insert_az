package ch.hostettler;

import java.util.concurrent.atomic.AtomicLong;

final class Result {

    Thread source;
    final long elapsed;
    final int count;
    final AtomicLong counter;
    
    public Result(AtomicLong counter, int count, long elapsed) {
        this.source = Thread.currentThread();
        this.elapsed = elapsed;
        this.count = count;
        this.counter = counter;
    }

    void log() {
        long total = counter.addAndGet(count);
        System.out.printf("[%s] Inserted %d rows in %d ms (%f rows/sec) (total rows inserted=%d)%n", source.getName(), count, elapsed, 1000d * ((double) count / (double) elapsed), total);
    }
}