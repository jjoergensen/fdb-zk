package com.ph14.fdb.zk.session;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.Watcher.Event.EventType;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.collect.ImmutableMap;
import org.apache.zookeeper.data.Stat;
import com.ph14.fdb.zk.layer.StatKey;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbPath;
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

  public CompletableFuture<Void> removeAllSessionData(long sessionId) {
    return fdb.runAsync(tr -> {
      List<CompletableFuture<Void>> deletions = new ArrayList<>();

      deletions.add(watchEventChangefeed.clearAllWatchesForSession(tr, sessionId));

      // Sequential: several ephemerals can share a parent, whose counters are read-modify-written.
      for (String zkPath : fdbEphemeralNodeManager.getEphemeralNodeZkPaths(tr, sessionId).join()) {
        DirectorySubspace node;
        try {
          node = fdbNodeReader.getNodeDirectory(tr, zkPath).join();
        } catch (CompletionException e) {
          if (e.getCause() instanceof NoSuchDirectoryException) {
            continue;
          }
          throw e;
        }
        Stat stat = fdbNodeReader.getNodeStat(node, tr, StatKey.EPHEMERAL_OWNER).join();
        if (stat.getEphemeralOwner() != sessionId) {
          // Deleted and re-created by another session: not ours to remove.
          continue;
        }
        DirectorySubspace parent = fdbNodeReader.getNodeDirectory(tr, FdbPath.toZkParentPath(zkPath)).join();
        Stat parentStat = fdbNodeReader.getNodeStat(parent, tr, StatKey.CVERSION, StatKey.NUM_CHILDREN).join();
        fdbNodeWriter.writeStat(tr, parent, ImmutableMap.of(
            StatKey.PZXID, FdbNodeWriter.VERSIONSTAMP_FLAG,
            StatKey.CVERSION, parentStat.getCversion() + 1L,
            StatKey.NUM_CHILDREN, Math.max(0L, parentStat.getNumChildren() - 1L)));
        fdbNodeWriter.deleteNodeAsync(tr, node).join();
        watchEventChangefeed.appendToChangefeed(tr, EventType.NodeDeleted, zkPath).join();
        watchEventChangefeed.appendToChangefeed(tr, EventType.NodeChildrenChanged, FdbPath.toZkParentPath(zkPath)).join();
      }

      fdbEphemeralNodeManager.clearEphemeralNodesForSession(tr, sessionId);

      deletions.add(fdbSessionManager.removeSessionAsync(tr, sessionId));

      return AsyncUtil.whenAll(deletions);
    });
  }

}
