package com.ph14.fdb.zk.layer.ephemeral;

import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class FdbEphemeralNodeManager {

  private static final String EPHEMERAL_NODE_SUBSPACE = "fdb-zk-ephemeral-nodes";
  private static final byte[] EMPTY_VALUE = new byte[0];

  private final com.apple.foundationdb.subspace.Subspace ephemeralNodes;

  @Inject
  public FdbEphemeralNodeManager() {
    this.ephemeralNodes = com.ph14.fdb.zk.FdbZkKeyspace.raw().subspace(Tuple.from(EPHEMERAL_NODE_SUBSPACE));
  }

  public void addEphemeralNode(Transaction transaction, String zkPath, long sessionId) {
    transaction.set(ephemeralNodes.pack(Tuple.from(sessionId, zkPath)), EMPTY_VALUE);
  }

  public CompletableFuture<Iterable<String>> getEphemeralNodeZkPaths(Transaction transaction, long sessionId) {
    return transaction.getRange(Range.startsWith(ephemeralNodes.pack(Tuple.from(sessionId))))
        .asList()
        .thenApply(kvs -> Iterables.transform(
            kvs,
            kv -> ephemeralNodes.unpack(kv.getKey()).getString(1)));
  }

  public void removeNode(Transaction transaction, String zkPath, long sessionId) {
    transaction.clear(ephemeralNodes.pack(Tuple.from(sessionId, zkPath)));
  }

  public void clearEphemeralNodesForSession(Transaction transaction, long sessionId) {
    transaction.clear(Range.startsWith(ephemeralNodes.pack(Tuple.from(sessionId))));
  }

}
