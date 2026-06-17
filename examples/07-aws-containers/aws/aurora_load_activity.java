// AWS shape: "a Glue job or Lambda INSERTs the import batch into Aurora/RDS, and
// you pray the orchestrator never re-runs it." Temporal makes the re-run a
// FIRST-CLASS, EXPECTED event: Activities are at-least-once, so the `load` step
// can fire twice (retry, timeout, Worker crash mid-batch). The translation is:
//
//   AWS Glue/Lambda writes to Aurora  ->  an idempotent Temporal `load` Activity
//
// "Idempotent" here means: derive a STABLE idempotency key from the Workflow
// (workflowId + a deterministic batch index), wrap the batch in ONE transaction,
// and use INSERT ... ON CONFLICT (idempotency_key) DO NOTHING so a second
// attempt is a no-op. at-least-once Activity + idempotent SQL = effectively-once
// writes. This is the relational sibling of the DynamoDB conditional-write
// example (dynamodb_idempotency.java) — same lesson, SQL instead of NoSQL.
//
// CONNECTION MANAGEMENT: a Worker runs MANY Activity threads concurrently, so do
// NOT open a connection per Activity. Use a bounded pool (HikariCP) sized
// RELATIVE TO activity concurrency, and write to the Aurora WRITER endpoint.
// Credentials come from SSM / IAM DB auth at startup — never hardcoded.
//
// DataSource sits behind a field so the Temporal usage is illustrative; the JDBC
// calls are the real shape you'd run against Aurora.
package aws;

import io.temporal.activity.Activity;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import javax.sql.DataSource;

@ActivityInterface
interface AuroraLoadActivities {
  @ActivityMethod
  long load(String workflowId, List<OrderRow> rows); // returns rows actually inserted
}

record OrderRow(String orderId, long amountCents) {}

class AuroraLoadActivitiesImpl implements AuroraLoadActivities {

  // A SHARED, BOUNDED pool — one per Worker, NOT one connection per Activity.
  // Size it relative to activity concurrency: HikariCP maximumPoolSize should be
  // ~ the Worker's maxConcurrentActivityExecutionSize (and the SUM across all
  // Worker replicas must stay under Aurora's max_connections — an unbounded pool
  // exhausts the cluster). See `pool()` below for how it's built.
  private final DataSource pool;

  AuroraLoadActivitiesImpl(DataSource pool) {
    this.pool = pool;
  }

  @Override
  public long load(String workflowId, List<OrderRow> rows) {
    // INSERT ... ON CONFLICT DO NOTHING: the per-row dedupe. The UNIQUE
    // constraint on idempotency_key (see aurora_schema.sql) is what makes the
    // second attempt a no-op instead of a duplicate.
    String sql =
        "INSERT INTO loaded_rows (idempotency_key, order_id, amount_cents) "
            + "VALUES (?, ?, ?) ON CONFLICT (idempotency_key) DO NOTHING";

    try (Connection conn = pool.getConnection()) {
      // ONE transaction for the whole batch. Turn OFF auto-commit, or every row
      // commits on its own and a crash leaves the batch half-loaded — defeating
      // the atomicity the transaction exists to give you.
      conn.setAutoCommit(false);
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        for (int i = 0; i < rows.size(); i++) {
          OrderRow row = rows.get(i);
          // STABLE key: workflowId + a deterministic batch index. The SAME input
          // always yields the SAME key across attempts, so a retry conflicts
          // harmlessly. NEVER derive it from System.currentTimeMillis() or a
          // random UUID — that would make every attempt look "new".
          String idempotencyKey = workflowId + ":" + i;
          ps.setString(1, idempotencyKey);
          ps.setString(2, row.orderId());
          ps.setLong(3, row.amountCents());
          ps.addBatch();
          Activity.getExecutionContext().heartbeat(i); // progress for large batches
        }
        int[] results = ps.executeBatch();
        conn.commit(); // commit the batch atomically; on throw we fall to rollback

        long inserted = 0;
        for (int r : results) {
          if (r > 0) inserted++; // ON CONFLICT rows report 0 — they were dedupe'd
        }
        return inserted;
      } catch (RuntimeException | java.sql.SQLException e) {
        conn.rollback(); // partial batch is discarded; the retry re-runs it cleanly
        throw new RuntimeException("load batch failed, rolled back", e);
      }
    } catch (java.sql.SQLException e) {
      // Let Temporal retry (at-least-once). The ON CONFLICT above guarantees the
      // retry can't duplicate already-committed rows.
      throw new RuntimeException("aurora load failed", e);
    }
  }

  // Build the pool ONCE at Worker startup. Credentials come from SSM Parameter
  // Store or, better, IAM DB auth — NO password in source (see ssm_parameter_config.java
  // and the lab-8 forward-reference). Point the JDBC URL at the Aurora WRITER
  // endpoint: writes must go to the writer; the reader endpoint is for queries.
  //
  //   HikariConfig cfg = new HikariConfig();
  //   cfg.setJdbcUrl("jdbc:postgresql://<cluster>.cluster-xxxx.us-east-1.rds.amazonaws.com:5432/imports");
  //   cfg.setUsername(ssm.get("/temporal-training/aurora/user"));
  //   cfg.setPassword(ssm.getSecure("/temporal-training/aurora/password")); // or an IAM auth token
  //   cfg.setMaximumPoolSize(20); // ~ maxConcurrentActivityExecutionSize, and sum-of-replicas < max_connections
  //   return new HikariDataSource(cfg);
  //
  // Stretch: replace the static password with a short-lived IAM DB auth token
  // (RdsUtilities.generateAuthenticationToken) so there is no stored secret at all.
}
