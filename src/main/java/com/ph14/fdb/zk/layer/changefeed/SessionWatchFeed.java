package com.ph14.fdb.zk.layer.changefeed;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxn.DisconnectReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.tuple.Tuple;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ph14.fdb.zk.FdbZkKeyspace;

@Singleton
public class SessionWatchFeed {

  private static final Logger LOG = LoggerFactory.getLogger(SessionWatchFeed.class);
  private static final int BATCH_SIZE = 128;

  private final Database database;
  private final ConcurrentHashMap<Long, Subscription> subscriptions = new ConcurrentHashMap<>();

  @Inject
  public SessionWatchFeed(Database database) {
    this.database = database;
  }

  public synchronized Runnable connect(long sessionId, Watcher watcher, BooleanSupplier isConnected) {
    Subscription subscription = new Subscription(sessionId, watcher, isConnected);
    database.run(tr -> {
      tr.set(WatchEventChangefeed.getConnectionKey(sessionId), subscription.owner);
      tr.clear(WatchEventChangefeed.getDeliveredRange(sessionId));
      return null;
    });
    Subscription previous = subscriptions.put(sessionId, subscription);
    if (previous != null) {
      previous.stop();
    }
    // The connect response must be queued before any notifications on the new connection.
    return subscription::drain;
  }

  public void shutdown() {
    subscriptions.values().forEach(Subscription::stop);
  }

  public void processRequest(long sessionId, Runnable action) {
    Subscription subscription = subscriptions.get(sessionId);
    if (subscription == null) {
      action.run();
    } else {
      // A notification must follow the reply that installs its client-side watch.
      synchronized (subscription) {
        action.run();
      }
    }
  }

  private class Subscription {
    private final long sessionId;
    private final Watcher watcher;
    private final BooleanSupplier isConnected;
    private final byte[] owner = Tuple.from(UUID.randomUUID().toString()).pack();
    private volatile boolean stopped;
    private volatile Batch armed;

    Subscription(long sessionId, Watcher watcher, BooleanSupplier isConnected) {
      this.sessionId = sessionId;
      this.watcher = watcher;
      this.isConnected = isConnected;
    }

    private boolean active() {
      return !stopped && subscriptions.get(sessionId) == this && isConnected.getAsBoolean();
    }

    private void drain() {
      if (!active()) {
        stop();
        return;
      }
      database.runAsync(tr -> tr.get(WatchEventChangefeed.getConnectionKey(sessionId)).thenCompose(currentOwner -> {
        if (!Arrays.equals(owner, currentOwner)) {
          return CompletableFuture.completedFuture((Batch) null);
        }
        return tr.get(WatchEventChangefeed.getTriggerKey(sessionId))
            .thenCompose(value -> tr.getRange(WatchEventChangefeed.getEventsRange(sessionId), BATCH_SIZE).asList())
            .thenApply(events -> new Batch(events, tr.watch(WatchEventChangefeed.getTriggerKey(sessionId)),
                tr.watch(WatchEventChangefeed.getConnectionKey(sessionId))));
      })).whenCompleteAsync((batch, error) -> {
        if (error != null) {
          fail(error);
          return;
        }
        if (batch == null) {
          stop();
          return;
        }
        armed = batch;
        if (!active()) {
          stop();
          return;
        }
        batch.ownerChanged.thenRun(this::stop);
        if (batch.events.isEmpty()) {
          CompletableFuture.anyOf(batch.changed, batch.ownerChanged).whenCompleteAsync((v, e) -> {
            batch.cancel();
            if (active()) {
              if (e == null) {
                drain();
              } else {
                fail(e);
              }
            }
          });
          return;
        }
        try {
          synchronized (this) {
            for (KeyValue event : batch.events) {
              if (!active()) {
                stop();
                return;
              }
              Tuple key = FdbZkKeyspace.raw().unpack(event.getKey());
              // Read journals written before ordinals were introduced as well.
              EventType type = EventType.fromInt((int) key.getLong(key.size() - 1));
              WatchedEvent notification = new WatchedEvent(type, KeeperState.SyncConnected, Tuple.fromBytes(event.getValue()).getString(0));
              if (watcher instanceof ServerCnxn) {
                ((ServerCnxn) watcher).process(notification, null);
              } else {
                watcher.process(notification);
              }
            }
          }
        } catch (Throwable deliveryFailure) {
          fail(deliveryFailure);
          return;
        }
        // A retry can repeat acknowledgements, never network sends. A displaced owner cannot erase the new owner's backlog.
        database.runAsync(tr -> tr.get(WatchEventChangefeed.getConnectionKey(sessionId)).thenApply(currentOwner -> {
          if (!Arrays.equals(owner, currentOwner) || !active()) {
            return false;
          }
          batch.events.forEach(event -> {
            WatchEventChangefeed.recordDelivery(tr, sessionId, event);
            tr.clear(event.getKey());
          });
          return true;
        })).whenCompleteAsync((acknowledged, ackError) -> {
          batch.cancel();
          if (ackError != null) {
            fail(ackError);
          } else if (acknowledged && active()) {
            drain();
          } else {
            stop();
          }
        });
      });
    }

    private void fail(Throwable error) {
      if (!stopped) {
        LOG.error("Watch delivery failed for session {}", Long.toHexString(sessionId), error);
      }
      stop();
    }

    private void stop() {
      stopped = true;
      subscriptions.remove(sessionId, this);
      Batch batch = armed;
      if (batch != null) {
        batch.cancel();
      }
      if (watcher instanceof ServerCnxn) {
        ((ServerCnxn) watcher).close(DisconnectReason.CLIENT_RECONNECT);
      }
    }
  }

  private static class Batch {
    private final List<KeyValue> events;
    private final CompletableFuture<Void> changed;
    private final CompletableFuture<Void> ownerChanged;

    Batch(List<KeyValue> events, CompletableFuture<Void> changed, CompletableFuture<Void> ownerChanged) {
      this.events = events;
      this.changed = changed;
      this.ownerChanged = ownerChanged;
    }

    void cancel() {
      changed.cancel(false);
      ownerChanged.cancel(false);
    }
  }
}
