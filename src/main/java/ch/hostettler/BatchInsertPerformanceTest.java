package ch.hostettler;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "insert", mixinStandardHelpOptions = true)
public class BatchInsertPerformanceTest implements Runnable {
    @Option(names = { "-u", "--url" }, required = true, description = "Database url")
    public String url;
    @Option(names = { "--username" }, required = true, description = "Database username")
    public String username;
    @Option(names = { "--password" }, required = true, description = "Database password")
    public String password;
    @Option(names = { "-t", "--threads" }, description = "Number of threads", defaultValue = "1")
    public int threads;
    @Option(names = { "-r", "--rows" }, description = "Number of rows", defaultValue = "5_000")
    public int rows;
    @Option(names = { "-b", "--batch-size" }, description = "Batch size", defaultValue = "500")
    public int batchSize;
    @Option(names = { "-c", "--commit-size" }, description = "Commit size", defaultValue = "500")
    public int commitSize;
    @Option(names = { "-s", "--column-store" }, description = "Create as column store table", defaultValue = "false")
    public boolean columnStoreTable;
    @Option(names = { "-x", "--temp-table" }, description = "Create a temp table", defaultValue = "false")
    public boolean tempTable;
    @Option(names = { "-z", "--bulk-mode" }, description = "Set the driver into bulk mode", defaultValue = "false")
    public boolean bulkMode;

    final AtomicLong counter = new AtomicLong(0L);
    final BlockingQueue<Result> queue = new LinkedBlockingQueue<>();

    public static void main(String[] args) {
        int exitCode = new CommandLine(new BatchInsertPerformanceTest()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {

        url = String.format("%s;useBulkCopyForBatchInsert=%s", url, bulkMode);
        System.out.println(String.format("Use URL : %s", url));

        new Latency().calculateLatency(this.url);
        PooledDataSource ds = new PooledDataSource(url, username, password);
        try {
            createTable(ds);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }

        long time = System.currentTimeMillis();
        List<Job> jobs = new ArrayList<>();
        int rowsPerThread = rows / threads;

        long start = 1L;

        for (int i = 0; i < threads; i++) {
            int rowCount = rowsPerThread + (i == 0 ? rows % threads : 0);
            jobs.add(new Job(counter, queue, ds, batchSize, commitSize, tempTable, rowCount, start));
            System.out.printf("Created job#%d with %d rows%n", i, rowCount);
            start += rowCount;
        }

        Logger logger = new Logger(queue);
        CompletableFuture<Void> loggerFuture = CompletableFuture.runAsync(logger);

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        System.out.printf("Created executor with %d threads%n", threads);
        try {
            for (Future<?> future : executor.invokeAll(jobs)) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    System.err.printf("An error occurred %s%n", e.getCause().getMessage());
                    e.getCause().printStackTrace(System.err);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdown();
        }
        logger.done = true;
        loggerFuture.join();

        time = System.currentTimeMillis() - time;
        double avg = (double) rows;
        avg = rows / ((double) time) * 1_000;

        System.out.println("Inserted " + rows + " in " + ((double) time / (double) 1_000) + "s      avg=" + avg + " rows/s");

    }

    private void createTable(PooledDataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            Statement stmt = conn.createStatement();
            System.out.println(String.format("DROP TABLE : %s", Statements.getDropTableStatement(tempTable)));
            stmt.executeUpdate(Statements.getDropTableStatement(tempTable));
            System.out.println(String.format("CREATE TABLE : %s", Statements.getCreateTableStatement(tempTable, columnStoreTable)));
            stmt.executeUpdate(Statements.getCreateTableStatement(tempTable, columnStoreTable));
        }
    }

}