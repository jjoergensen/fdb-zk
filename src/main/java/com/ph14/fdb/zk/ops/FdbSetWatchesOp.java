package com.ph14.fdb.zk.ops;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.layer.StatKey;

public class FdbSetWatchesOp implements FdbOp<SetWatches, List<WatchedEvent>> {

  private final FdbNodeReader fdbNodeReader;
  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbSetWatchesOp(FdbNodeReader fdbNodeReader,
                         FdbWatchManager fdbWatchManager) {
    this.fdbNodeReader = fdbNodeReader;
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public CompletableFuture<Result<List<WatchedEvent>, KeeperException>> execute(Request zkRequest, Transaction transaction, SetWatches setWatches) {
    Watcher watcher = zkRequest.cnxn;

    List<CompletableFuture<Optional<WatchedEvent>>> backfilledWatches = new ArrayList<>(
        setWatches.getDataWatches().size()
            + setWatches.getExistWatches().size()
            + setWatches.getChildWatches().size());

    for (String path : setWatches.getDataWatches()) {
      backfilledWatches.add(
          readNodeStat(transaction, path, StatKey.MZXID)
              .thenApply(stat -> {
                if (!stat.isPresent()) {
                  return Optional.of(new WatchedEvent(EventType.NodeDeleted,
                      KeeperState.SyncConnected, path));
                } else if (stat.get().getMzxid() > setWatches.getRelativeZxid()) {
                  return Optional.of(new WatchedEvent(EventType.NodeDataChanged,
                      KeeperState.SyncConnected, path));
                } else {
                  fdbWatchManager.addNodeDataUpdatedWatch(transaction, path, watcher, zkRequest.sessionId);
                  fdbWatchManager.addNodeDeletedWatch(transaction, path, watcher, zkRequest.sessionId);
                  return Optional.<WatchedEvent>empty();
                }
              })
      );
    }

    for (String path : setWatches.getExistWatches()) {
      backfilledWatches.add(
          readNodeStat(transaction, path, StatKey.CZXID)
              .thenApply(stat -> {
                if (stat.isPresent()) {
                  return Optional.of(new WatchedEvent(EventType.NodeCreated,
                      KeeperState.SyncConnected, path));
                } else {
                  fdbWatchManager.addNodeCreatedWatch(transaction, path, watcher, zkRequest.sessionId);
                  return Optional.<WatchedEvent>empty();
                }
              })
      );
    }

    for (String path : setWatches.getChildWatches()) {
      backfilledWatches.add(
          readNodeStat(transaction, path, StatKey.PZXID)
              .thenApply(stat -> {
                if (!stat.isPresent()) {
                  return Optional.of(new WatchedEvent(EventType.NodeDeleted,
                      KeeperState.SyncConnected, path));
                } else if (stat.get().getPzxid() > setWatches.getRelativeZxid()) {
                  return Optional.of(new WatchedEvent(EventType.NodeChildrenChanged,
                      KeeperState.SyncConnected, path));
                } else {
                  fdbWatchManager.addNodeChildrenWatch(transaction, path, watcher, zkRequest.sessionId);
                  fdbWatchManager.addNodeDeletedWatch(transaction, path, watcher, zkRequest.sessionId);
                  return Optional.<WatchedEvent>empty();
                }
              })
      );
    }

    return AsyncUtil.getAll(backfilledWatches).thenApply(events -> Result.ok(events.stream()
        .flatMap(Optional::stream)
        .collect(Collectors.toList())));
  }

  private CompletableFuture<Optional<Stat>> readNodeStat(Transaction transaction, String zkPath, StatKey ... statKeys) {
    List<String> fdbPath = FdbPath.toFdbPath(zkPath);

    return com.ph14.fdb.zk.FdbZkKeyspace.dir().exists(transaction, fdbPath)
        .thenCompose(exists -> exists
            ? com.ph14.fdb.zk.FdbZkKeyspace.dir().open(transaction, fdbPath)
                .thenCompose(nodeSubspace -> fdbNodeReader.getNodeStat(nodeSubspace, transaction, statKeys))
                .thenApply(Optional::of)
            : CompletableFuture.completedFuture(Optional.empty()));
  }

}
