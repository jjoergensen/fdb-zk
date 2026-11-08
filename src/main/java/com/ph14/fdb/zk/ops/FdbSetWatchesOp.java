package com.ph14.fdb.zk.ops;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SetWatches2;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.layer.StatKey;

public class FdbSetWatchesOp implements FdbOp<SetWatches, Boolean> {

  private final FdbNodeReader fdbNodeReader;
  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbSetWatchesOp(FdbNodeReader fdbNodeReader,
                         FdbWatchManager fdbWatchManager) {
    this.fdbNodeReader = fdbNodeReader;
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public CompletableFuture<Result<Boolean, KeeperException>> execute(Request zkRequest, Transaction transaction, SetWatches setWatches) {
    List<CompletableFuture<Optional<WatchedEvent>>> backfilledWatches = new ArrayList<>(
        setWatches.getDataWatches().size()
            + setWatches.getExistWatches().size()
            + setWatches.getChildWatches().size());

    for (String path : setWatches.getDataWatches()) {
      backfilledWatches.add(
          fdbWatchManager.wasNotified(transaction, path, zkRequest.sessionId, WatcherType.Data)
              .thenCompose(notified -> notified ? CompletableFuture.completedFuture(Optional.<WatchedEvent>empty())
                  : readNodeStat(transaction, path, StatKey.MZXID)
                      .thenApply(stat -> {
                        if (!stat.isPresent()) {
                          return Optional.of(new WatchedEvent(EventType.NodeDeleted,
                              KeeperState.SyncConnected, path));
                        } else if (stat.get().getMzxid() > setWatches.getRelativeZxid()) {
                          return Optional.of(new WatchedEvent(EventType.NodeDataChanged,
                              KeeperState.SyncConnected, path));
                        } else {
                          fdbWatchManager.addDataWatch(transaction, path, zkRequest.sessionId);
                          return Optional.<WatchedEvent>empty();
                        }
                      }))
      );
    }

    for (String path : setWatches.getExistWatches()) {
      backfilledWatches.add(
          fdbWatchManager.wasNotified(transaction, path, zkRequest.sessionId, WatcherType.Data)
              .thenCompose(notified -> notified ? CompletableFuture.completedFuture(Optional.<WatchedEvent>empty())
                  : readNodeStat(transaction, path, StatKey.CZXID)
                      .thenApply(stat -> {
                        if (stat.isPresent()) {
                          return Optional.of(new WatchedEvent(EventType.NodeCreated,
                              KeeperState.SyncConnected, path));
                        } else {
                          fdbWatchManager.addDataWatch(transaction, path, zkRequest.sessionId);
                          return Optional.<WatchedEvent>empty();
                        }
                      }))
      );
    }

    for (String path : setWatches.getChildWatches()) {
      backfilledWatches.add(
          fdbWatchManager.wasNotified(transaction, path, zkRequest.sessionId, WatcherType.Children)
              .thenCompose(notified -> notified ? CompletableFuture.completedFuture(Optional.<WatchedEvent>empty())
                  : readNodeStat(transaction, path, StatKey.PZXID)
                      .thenApply(stat -> {
                        if (!stat.isPresent()) {
                          return Optional.of(new WatchedEvent(EventType.NodeDeleted,
                              KeeperState.SyncConnected, path));
                        } else if (stat.get().getPzxid() > setWatches.getRelativeZxid()) {
                          return Optional.of(new WatchedEvent(EventType.NodeChildrenChanged,
                              KeeperState.SyncConnected, path));
                        } else {
                          fdbWatchManager.addChildWatch(transaction, path, zkRequest.sessionId);
                          return Optional.<WatchedEvent>empty();
                        }
                      }))
      );
    }

    return AsyncUtil.getAll(backfilledWatches).thenApply(events -> {
      Set<Tuple> seen = new HashSet<>();
      events.stream().flatMap(Optional::stream).forEach(event -> {
        if (seen.add(Tuple.from(event.getPath(), event.getType().getIntValue()))) {
          fdbWatchManager.restoreEvent(transaction, event.getPath(), zkRequest.sessionId, event.getType());
        }
      });
      return Result.ok(true);
    });
  }

  public CompletableFuture<Result<Boolean, KeeperException>> execute(Request zkRequest, Transaction transaction, SetWatches2 setWatches) {
    for (String path : setWatches.getPersistentWatches()) {
      fdbWatchManager.addPersistentWatch(transaction, path, zkRequest.sessionId, AddWatchMode.PERSISTENT.getMode());
    }
    for (String path : setWatches.getPersistentRecursiveWatches()) {
      fdbWatchManager.addPersistentWatch(transaction, path, zkRequest.sessionId, AddWatchMode.PERSISTENT_RECURSIVE.getMode());
    }
    return execute(zkRequest, transaction, new SetWatches(setWatches.getRelativeZxid(), setWatches.getDataWatches(),
        setWatches.getExistWatches(), setWatches.getChildWatches()));
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
