package ch.hostettler;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class Result {
    protected static final Logger LOGGER = LogManager.getLogger();
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
        LOGGER.info(String.format("[%s] Inserted %,d rows in %,d ms (%,.2f rows/sec) (total rows inserted=%,d)", source.getName(), count, elapsed, 1000d * ((double) count / (double) elapsed), total));
    }
}