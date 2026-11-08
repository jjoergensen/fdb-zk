package com.ph14.fdb.zk.layer;

import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;

import com.apple.foundationdb.Transaction;
import com.google.inject.Inject;
import com.ph14.fdb.zk.layer.changefeed.WatchEventChangefeed;

public class FdbWatchManager {

  private final WatchEventChangefeed watchChangefeed;

  @Inject
  public FdbWatchManager(WatchEventChangefeed watchChangefeed) {
    this.watchChangefeed = watchChangefeed;
  }

  public void triggerNodeCreatedWatch(Transaction transaction, String zkPath) {
    watchChangefeed.appendToChangefeed(transaction, EventType.NodeCreated, zkPath).join();
  }

  public void triggerNodeUpdatedWatch(Transaction transaction, String zkPath) {
    watchChangefeed.appendToChangefeed(transaction, EventType.NodeDataChanged, zkPath).join();
  }

  public void triggerNodeDeletedWatch(Transaction transaction, String zkPath) {
    watchChangefeed.appendToChangefeed(transaction, EventType.NodeDeleted, zkPath).join();
  }

  public void triggerNodeChildrenWatch(Transaction transaction, String zkPath) {
    watchChangefeed.appendToChangefeed(transaction, EventType.NodeChildrenChanged, zkPath).join();
  }

  public void addDataWatch(Transaction transaction, String zkPath, long sessionId) {
    watchChangefeed.addWatch(transaction, sessionId, zkPath, WatcherType.Data);
  }

  public void addChildWatch(Transaction transaction, String zkPath, long sessionId) {
    watchChangefeed.addWatch(transaction, sessionId, zkPath, WatcherType.Children);
  }

  public void addPersistentWatch(Transaction transaction, String zkPath, long sessionId, int mode) {
    if (mode == AddWatchMode.PERSISTENT.getMode()) {
      watchChangefeed.addWatch(transaction, sessionId, zkPath, WatcherType.Persistent);
    } else if (mode == AddWatchMode.PERSISTENT_RECURSIVE.getMode()) {
      watchChangefeed.addWatch(transaction, sessionId, zkPath, WatcherType.PersistentRecursive);
    } else {
      throw new IllegalArgumentException("Unknown watch mode: " + mode);
    }
  }

  public CompletableFuture<Boolean> checkWatch(Transaction transaction, String zkPath, long sessionId, WatcherType type, boolean remove) {
    return watchChangefeed.checkWatch(transaction, sessionId, zkPath, type, remove);
  }

  public CompletableFuture<Boolean> wasNotified(Transaction transaction, String zkPath, long sessionId, WatcherType type) {
    return watchChangefeed.wasNotified(transaction, sessionId, zkPath, type);
  }

  public void restoreEvent(Transaction transaction, String zkPath, long sessionId, EventType type) {
    if (type != EventType.NodeChildrenChanged) {
      checkWatch(transaction, zkPath, sessionId, WatcherType.Data, true).join();
    }
    if (type == EventType.NodeChildrenChanged || type == EventType.NodeDeleted) {
      checkWatch(transaction, zkPath, sessionId, WatcherType.Children, true).join();
    }
    watchChangefeed.enqueueEvent(transaction, sessionId, type, zkPath);
  }
}
