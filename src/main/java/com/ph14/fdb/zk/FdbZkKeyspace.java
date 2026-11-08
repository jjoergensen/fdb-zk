package com.ph14.fdb.zk;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.Directory;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Everything this layer stores lives inside one Directory Layer partition, fdb-zk/<namespace> (env FDB_ZK_NAMESPACE).
 * The upstream layer used the cluster-wide root directory and raw root keys; on a shared cluster that mixes ZooKeeper
 * paths with other applications' directories and makes two independent ZK "clusters" impossible.
 * Delete the partition to remove one ZK cluster completely.
 */
public final class FdbZkKeyspace {
  private static volatile Directory directory;
  private static volatile DirectorySubspace raw;

  private FdbZkKeyspace() {}

  public static synchronized void init(Database db) {
    if (directory != null) return;
    String ns = System.getenv().getOrDefault("FDB_ZK_NAMESPACE", "default");
    DirectorySubspace partition = db.run(tr -> DirectoryLayer.getDefault()
        .createOrOpen(tr, List.of("fdb-zk", ns), "partition".getBytes(StandardCharsets.UTF_8)).join());
    directory = partition;
    raw = db.run(tr -> partition.createOrOpen(tr, List.of("raw")).join());
  }

  /** Use instead of DirectoryLayer.getDefault(). */
  public static Directory dir() {
    if (directory == null) throw new IllegalStateException("FdbZkKeyspace.init not called");
    return directory;
  }

  /** Prefix subspace for the layer's raw (non-directory) keys: watches, changefeed, ephemeral index. */
  public static DirectorySubspace raw() {
    if (raw == null) throw new IllegalStateException("FdbZkKeyspace.init not called");
    return raw;
  }
}
