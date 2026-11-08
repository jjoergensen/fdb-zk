package com.ph14.fdb.zk.layer.changefeed;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.inject.Singleton;
import com.ph14.fdb.zk.FdbZkKeyspace;

@Singleton
public class WatchEventChangefeed {

  private static final String ACTIVE_WATCH_NAMESPACE = "fdb-zk-watch-registration";
  private static final String SESSION_WATCH_NAMESPACE = "fdb-zk-watch-session";
  private static final String CHANGEFEED_TRIGGER_NAMESPACE = "fdb-zk-watch-trigger";
  private static final String CHANGEFEED_NAMESPACE = "fdb-zk-watch-cf";
  private static final String CONNECTION_NAMESPACE = "fdb-zk-watch-connection";
  private static final String DELIVERED_NAMESPACE = "fdb-zk-watch-delivered";
  private static final byte[] EMPTY_VALUE = new byte[0];

  private final Map<Transaction, Long> eventOrdinals = new WeakHashMap<>();

  public void addWatch(Transaction transaction, long sessionId, String path, WatcherType type) {
    transaction.set(getActiveWatchKey(sessionId, path, type), EMPTY_VALUE);
    transaction.set(getSessionWatchKey(sessionId, path, type), EMPTY_VALUE);
    transaction.clear(getDeliveredKey(sessionId, path, type));
  }

  public CompletableFuture<Boolean> wasNotified(Transaction transaction, long sessionId, String path, WatcherType type) {
    // Reconnect restoration can race both the drain and its acknowledgement. Check both in one snapshot.
    return transaction.get(getDeliveredKey(sessionId, path, type)).thenCompose(delivered -> delivered != null
        ? CompletableFuture.completedFuture(true)
        : transaction.getRange(getEventsRange(sessionId)).asList().thenApply(events -> events.stream().anyMatch(event -> {
          Tuple key = FdbZkKeyspace.raw().unpack(event.getKey());
          return Tuple.fromBytes(event.getValue()).getString(0).equals(path)
              && matches(type, EventType.fromInt((int) key.getLong(key.size() - 1)));
        })));
  }

  public void enqueueEvent(Transaction transaction, long sessionId, EventType eventType, String path) {
    synchronized (transaction) {
      enqueueEvent(transaction, sessionId, eventType, path, nextOrdinal(transaction));
    }
  }

  private long nextOrdinal(Transaction transaction) {
    synchronized (eventOrdinals) {
      return eventOrdinals.merge(transaction, 1L, Long::sum);
    }
  }

  private void enqueueEvent(Transaction transaction, long sessionId, EventType eventType, String path, long ordinal) {
    Tuple key = Tuple.from(CHANGEFEED_NAMESPACE, sessionId, Versionstamp.incomplete(), ordinal, eventType.getIntValue());
    transaction.mutate(MutationType.SET_VERSIONSTAMPED_KEY, packWithVersionstamp(key, FdbZkKeyspace.raw().getKey()), Tuple.from(path).pack());
    transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, getTriggerKey(sessionId),
        packWithVersionstamp(Tuple.from(Versionstamp.incomplete()), null));
  }

  public CompletableFuture<Boolean> checkWatch(Transaction transaction, long sessionId, String path,
                                                WatcherType type, boolean remove) {
    return transaction.getRange(FdbZkKeyspace.raw().range(Tuple.from(SESSION_WATCH_NAMESPACE, sessionId, path)))
        .asList().thenApply(registrations -> {
          boolean found = false;
          for (KeyValue registration : registrations) {
            WatcherType registeredType = WatcherType.fromInt((int) FdbZkKeyspace.raw().unpack(registration.getKey()).getLong(3));
            if (type == WatcherType.Any || type == registeredType) {
              found = true;
              if (remove) {
                transaction.clear(registration.getKey());
                transaction.clear(getActiveWatchKey(sessionId, path, registeredType));
              }
            }
          }
          return found;
        });
  }

  public CompletableFuture<Void> appendToChangefeed(Transaction transaction, EventType eventType, String path) {
    // Purges can emit concurrently in one transaction. Serialize reads and consumption of one-shot registrations.
    synchronized (transaction) {
      long ordinal = nextOrdinal(transaction);
      List<KeyValue> registrations = new ArrayList<>(transaction.getRange(
          FdbZkKeyspace.raw().range(Tuple.from(ACTIVE_WATCH_NAMESPACE, path))).asList().join());
      if (eventType != EventType.NodeChildrenChanged) {
        String ancestor = path;
        while (!ancestor.equals("/")) {
          int slash = ancestor.lastIndexOf('/');
          ancestor = slash == 0 ? "/" : ancestor.substring(0, slash);
          registrations.addAll(transaction.getRange(FdbZkKeyspace.raw().range(
              Tuple.from(ACTIVE_WATCH_NAMESPACE, ancestor, WatcherType.PersistentRecursive.getIntValue()))).asList().join());
        }
      }

      Set<Long> recipients = new HashSet<>();
      for (KeyValue registration : registrations) {
        Tuple key = FdbZkKeyspace.raw().unpack(registration.getKey());
        String watchedPath = key.getString(1);
        WatcherType type = WatcherType.fromInt((int) key.getLong(2));
        long sessionId = key.getLong(3);
        if (matches(type, eventType)) {
          recipients.add(sessionId);
          if (type == WatcherType.Data || type == WatcherType.Children) {
            transaction.clear(registration.getKey());
            transaction.clear(getSessionWatchKey(sessionId, watchedPath, type));
          }
        }
      }

      for (long sessionId : recipients) {
        // All matching registrations share one notification, but distinct mutations retain their ordinals.
        enqueueEvent(transaction, sessionId, eventType, path, ordinal);
      }
      return CompletableFuture.completedFuture(null);
    }
  }

  byte[] packWithVersionstamp(Tuple tuple, byte[] prefix) {
    return tuple.packWithVersionstamp(prefix);
  }

  private static boolean matches(WatcherType type, EventType eventType) {
    switch (type) {
      case Data:
        return eventType == EventType.NodeCreated || eventType == EventType.NodeDataChanged || eventType == EventType.NodeDeleted;
      case Children:
        return eventType == EventType.NodeChildrenChanged || eventType == EventType.NodeDeleted;
      case Persistent:
        return true;
      case PersistentRecursive:
        return eventType != EventType.NodeChildrenChanged;
      default:
        return false;
    }
  }

  public CompletableFuture<Void> clearAllWatchesForSession(Transaction transaction, long sessionId) {
    Range registrations = FdbZkKeyspace.raw().range(Tuple.from(SESSION_WATCH_NAMESPACE, sessionId));
    return transaction.getRange(registrations).asList().thenAccept(keys -> {
      for (KeyValue keyValue : keys) {
        Tuple key = FdbZkKeyspace.raw().unpack(keyValue.getKey());
        transaction.clear(getActiveWatchKey(sessionId, key.getString(2), WatcherType.fromInt((int) key.getLong(3))));
      }
      transaction.clear(registrations);
      transaction.clear(getEventsRange(sessionId));
      transaction.clear(getTriggerKey(sessionId));
      transaction.clear(getConnectionKey(sessionId));
      transaction.clear(getDeliveredRange(sessionId));
    });
  }

  static Range getEventsRange(long sessionId) {
    return FdbZkKeyspace.raw().range(Tuple.from(CHANGEFEED_NAMESPACE, sessionId));
  }

  static byte[] getTriggerKey(long sessionId) {
    return FdbZkKeyspace.raw().pack(Tuple.from(CHANGEFEED_TRIGGER_NAMESPACE, sessionId));
  }

  static byte[] getConnectionKey(long sessionId) {
    return FdbZkKeyspace.raw().pack(Tuple.from(CONNECTION_NAMESPACE, sessionId));
  }

  static Range getDeliveredRange(long sessionId) {
    return FdbZkKeyspace.raw().range(Tuple.from(DELIVERED_NAMESPACE, sessionId));
  }

  static void recordDelivery(Transaction transaction, long sessionId, KeyValue event) {
    Tuple key = FdbZkKeyspace.raw().unpack(event.getKey());
    EventType type = EventType.fromInt((int) key.getLong(key.size() - 1));
    String path = Tuple.fromBytes(event.getValue()).getString(0);
    for (WatcherType watchType : new WatcherType[] {WatcherType.Data, WatcherType.Children}) {
      if (matches(watchType, type) && transaction.get(getSessionWatchKey(sessionId, path, watchType)).join() == null) {
        transaction.set(getDeliveredKey(sessionId, path, watchType), EMPTY_VALUE);
      }
    }
  }

  private static byte[] getDeliveredKey(long sessionId, String path, WatcherType type) {
    return FdbZkKeyspace.raw().pack(Tuple.from(DELIVERED_NAMESPACE, sessionId, path, type.getIntValue()));
  }

  private static byte[] getActiveWatchKey(long sessionId, String path, WatcherType type) {
    return FdbZkKeyspace.raw().pack(Tuple.from(ACTIVE_WATCH_NAMESPACE, path, type.getIntValue(), sessionId));
  }

  private static byte[] getSessionWatchKey(long sessionId, String path, WatcherType type) {
    return FdbZkKeyspace.raw().pack(Tuple.from(SESSION_WATCH_NAMESPACE, sessionId, path, type.getIntValue()));
  }
}
