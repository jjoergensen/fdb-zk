package com.ph14.fdb.zk.session;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbRequestTransactions;
import com.ph14.fdb.zk.FdbTransactionState;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.StatKey;
import com.ph14.fdb.zk.layer.changefeed.WatchEventChangefeed;
import com.ph14.fdb.zk.layer.ephemeral.FdbEphemeralNodeManager;

@Singleton
public class FdbSessionDataPurger {

  private final Database fdb;
  private final WatchEventChangefeed watchEventChangefeed;
  private final FdbNodeWriter fdbNodeWriter;
  private final FdbNodeReader fdbNodeReader;
  private final FdbEphemeralNodeManager fdbEphemeralNodeManager;
  private final FdbSessionManager fdbSessionManager;

  @Inject
  public FdbSessionDataPurger(Database fdb,
                              WatchEventChangefeed watchEventChangefeed,
                              FdbNodeWriter fdbNodeWriter,
                              FdbNodeReader fdbNodeReader,
                              FdbEphemeralNodeManager fdbEphemeralNodeManager,
                              FdbSessionManager fdbSessionManager) {
    this.fdb = fdb;
    this.watchEventChangefeed = watchEventChangefeed;
    this.fdbNodeWriter = fdbNodeWriter;
    this.fdbNodeReader = fdbNodeReader;
    this.fdbEphemeralNodeManager = fdbEphemeralNodeManager;
    this.fdbSessionManager = fdbSessionManager;
  }

  public CompletableFuture<Result<Boolean, KeeperException>> close(Request request,
                                       FdbRequestTransactions transactions) {
    var result = transactions.run(request, tr -> {
      fdbSessionManager.beginClose(tr, request.sessionId, false);
      return Result.ok(Boolean.TRUE);
    });
    if (result.isErr()) return CompletableFuture.completedFuture(Result.err(result.unwrapErrOrElseThrow()));
    return purge(request.sessionId).thenApply(v -> Result.ok(Boolean.TRUE));
  }

  public CompletableFuture<Void> removeAllSessionData(long sessionId) {
    return removeExpiredSessionData(sessionId, false);
  }

  public CompletableFuture<Void> removeExpiredSessionData(long sessionId, boolean expiredOnly) {
    boolean closing = fdb.run(tr -> fdbSessionManager.beginClose(tr, sessionId, expiredOnly));
    return closing ? purge(sessionId) : CompletableFuture.completedFuture(null);
  }

  public void maintainLog(long now) {
    while (fdb.run(tr -> watchEventChangefeed.pruneLog(tr, now))) { }
  }

  private CompletableFuture<Void> purge(long sessionId) {
    return CompletableFuture.runAsync(() -> {
      while (!fdb.run(tr -> purgeBatch(tr, sessionId))) { }
    });
  }

  private boolean purgeBatch(Transaction tr, long sessionId) {
    tr.options().setTimeout(1000);
    if (tr.get(FdbSessionManager.closingKey(sessionId)).join() == null) return true;
    List<String> paths = fdbEphemeralNodeManager.getBatch(tr, sessionId, 1).join();
    if (paths.isEmpty()) {
      watchEventChangefeed.clearAllWatchesForSession(tr, sessionId).join();
      FdbRequestTransactions.clearReceipts(tr, sessionId);
      fdbSessionManager.removeSessionAsync(tr, sessionId).join();
      return true;
    }
    String path = paths.get(0);
    try {
      DirectorySubspace node = fdbNodeReader.getNodeDirectory(tr, path).join();
      Stat stat = fdbNodeReader.getNodeStat(node, tr, StatKey.EPHEMERAL_OWNER).join();
      if (stat.getEphemeralOwner() == sessionId) {
        DirectorySubspace parent = fdbNodeReader.getNodeDirectory(tr, FdbPath.toZkParentPath(path)).join();
        Stat parentStat = fdbNodeReader.getNodeStat(parent, tr, StatKey.CVERSION, StatKey.NUM_CHILDREN).join();
        fdbNodeWriter.writeStat(tr, parent, ImmutableMap.of(
            StatKey.PZXID, FdbNodeWriter.ZXID_FLAG,
            StatKey.CVERSION, parentStat.getCversion() + 1L,
            StatKey.NUM_CHILDREN, Math.max(0L, parentStat.getNumChildren() - 1L)));
        FdbTransactionState.of(tr).eventAcls.put(path, fdbNodeReader.readAcls(node, tr).join());
        FdbTransactionState.of(tr).eventAcls.put(FdbPath.toZkParentPath(path), fdbNodeReader.readAcls(parent, tr).join());
        fdbNodeWriter.deleteNodeAsync(tr, node).join();
        watchEventChangefeed.appendToChangefeed(tr, EventType.NodeDeleted, path).join();
        watchEventChangefeed.appendToChangefeed(tr, EventType.NodeChildrenChanged, FdbPath.toZkParentPath(path)).join();
      }
    } catch (CompletionException e) {
      if (!(e.getCause() instanceof NoSuchDirectoryException)) throw e;
    }
    fdbEphemeralNodeManager.removeNode(tr, path, sessionId);
    return false;
  }
}
