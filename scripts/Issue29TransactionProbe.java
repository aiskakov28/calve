package org.apache.iceberg.rest;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Types;

public class Issue29TransactionProbe {
  private static final FileIO IO = new FileIO() {
    public InputFile newInputFile(String path) { throw new UnsupportedOperationException(path); }
    public OutputFile newOutputFile(String path) { throw new UnsupportedOperationException(path); }
    public void deleteFile(String path) { throw new AssertionError("unexpected delete: " + path); }
  };

  private static class MemoryOps implements TableOperations {
    TableMetadata metadata;
    boolean injectConflict;
    int applied;
    int noops;

    MemoryOps() {
      Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
      metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
          "file:///tmp/issue29", Map.of());
    }

    public TableMetadata current() { return metadata; }
    public TableMetadata refresh() { return metadata; }
    public FileIO io() { return IO; }
    public String metadataFileLocation(String filename) { return "file:///tmp/issue29/" + filename; }
    public LocationProvider locationProvider() { return null; }

    public void commit(TableMetadata base, TableMetadata updated) {
      if (injectConflict) {
        injectConflict = false;
        metadata = TableMetadata.buildFrom(metadata).setProperties(Map.of("external", "1")).build();
        throw new CommitFailedException("forced concurrent metadata change");
      }
      if (base != metadata) {
        throw new CommitFailedException("base changed");
      }
      if (base == updated) {
        noops++;
      } else {
        metadata = updated;
        applied++;
      }
    }
  }

  private static int pending(Transaction transaction) throws Exception {
    Field field = BaseTransaction.class.getDeclaredField("updates");
    field.setAccessible(true);
    return ((List<?>) field.get(transaction)).size();
  }

  private static void stage(Transaction transaction, String key, boolean publicApi) {
    if (publicApi) {
      transaction.updateProperties().set(key, "1").commit();
    } else {
      TableOperations ops = ((HasTableOperations) transaction.table()).operations();
      CatalogHandlers.commit(ops, new UpdateTableRequest(
          List.of(), List.of(new MetadataUpdate.SetProperties(Map.of(key, "1")))));
    }
  }

  private static void freshConflict(boolean publicApi) throws Exception {
    MemoryOps ops = new MemoryOps();
    Transaction first = Transactions.newTransaction("table", ops);
    Transaction second = Transactions.newTransaction("table", ops);
    stage(first, "a", publicApi);
    stage(second, "b", publicApi);
    int pending = pending(second);
    first.commitTransaction();
    second.commitTransaction();
    boolean retained = "1".equals(ops.current().properties().get("b"));
    if (retained != publicApi || pending != (publicApi ? 1 : 0)) {
      throw new AssertionError("unexpected refresh result");
    }
    System.out.printf("refresh: publicApi=%s pending=%d bRetained=%s applied=%d noops=%d%n",
        publicApi, pending, retained, ops.applied, ops.noops);
  }

  private static void retryConflict(boolean publicApi) throws Exception {
    MemoryOps ops = new MemoryOps();
    Transaction transaction = Transactions.newTransaction("table", ops);
    stage(transaction, "requested", publicApi);
    int pending = pending(transaction);
    ops.injectConflict = true;
    transaction.commitTransaction();
    boolean retained = "1".equals(ops.current().properties().get("requested"));
    boolean external = "1".equals(ops.current().properties().get("external"));
    if (retained != publicApi || !external || pending != (publicApi ? 1 : 0)) {
      throw new AssertionError("unexpected retry result");
    }
    System.out.printf("retry: publicApi=%s pending=%d requestedRetained=%s external=%s noops=%d%n",
        publicApi, pending, retained, external, ops.noops);
  }

  public static void main(String[] args) throws Exception {
    System.out.println("transaction class: "
        + BaseTransaction.class.getProtectionDomain().getCodeSource().getLocation());
    System.out.println("handler class: "
        + CatalogHandlers.class.getProtectionDomain().getCodeSource().getLocation());
    freshConflict(false);
    freshConflict(true);
    retryConflict(false);
    retryConflict(true);
  }
}
