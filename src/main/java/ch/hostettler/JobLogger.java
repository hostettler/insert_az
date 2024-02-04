package ch.hostettler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

final class JobLogger implements Runnable {
    BlockingQueue<Result> queue;

    JobLogger(BlockingQueue<Result> queue) {
        this.queue = queue;
    }

    volatile boolean done;

    @Override
    public void run() {

        try {
            for (; !done || !queue.isEmpty();) {
                Result r = queue.poll(500, TimeUnit.MILLISECONDS);
                if (r != null) {
                    r.log();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}