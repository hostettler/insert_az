package ch.hostettler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

final class Job implements Callable<Boolean> {

    private final int rows;
    private long rowId;
    private PooledDataSource ds;
    private int batchSize;
    private int commitSize;
    private Queue<Result> queue;
    private AtomicLong counter;
    private boolean tempTable;

    public Job(AtomicLong counter, Queue<Result> queue, PooledDataSource ds, int batchSize, int commitSize, boolean tempTable, int rows, long rowId) {
        this.counter = counter;
        this.queue = queue;
        this.ds = ds;
        this.batchSize = batchSize;
        this.commitSize = commitSize;
        this.tempTable = tempTable;
        this.rows = rows;
        this.rowId = rowId;
    }

    @Override
    public Boolean call() throws SQLException {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);
            long t = System.currentTimeMillis();

            System.out.println(Statements.getInsertStatement(this.tempTable));
            try (PreparedStatement statement = conn.prepareStatement(Statements.getInsertStatement(this.tempTable))) {

                for (int i = 0; i < rows; i++) {
                    Statements.fillPreparedStatement(statement, rowId + i);
                    statement.addBatch();

                    if (i > 0 && i % batchSize == 0 || (i == rows - 1)) {
                        System.out.println("Execute Batch : " + i);
                        statement.executeBatch();
                        statement.clearBatch();
                        statement.clearWarnings();
                        long end = System.currentTimeMillis();
                        queue.add(new Result(counter, batchSize, end - t));
                        t = end;
                        System.out.println("End of Batch");
                    }

                    if (i > 0 && i % commitSize == 0 || (i == rows - 1)) {
                        System.out.println("Commit");
                        conn.commit();
                    }
                }

            }
        }
        return true;
    }

}
