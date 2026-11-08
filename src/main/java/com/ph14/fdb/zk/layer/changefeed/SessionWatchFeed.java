package com.ph14.fdb.zk.layer.changefeed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn.DisconnectReason;
import org.apache.zookeeper.server.ServerCnxn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.ph14.fdb.zk.FdbTransactionState;
import com.ph14.fdb.zk.session.FdbSessionManager;

@Singleton
public class SessionWatchFeed {
  private static final Logger LOG = LoggerFactory.getLogger(SessionWatchFeed.class);
  private static final int BATCH_SIZE = 64;
  private final Database database;
  private final FdbSessionManager sessions;
  private final WatchEventChangefeed changefeed;
  private final ConcurrentHashMap<Long, Subscription> subscriptions = new ConcurrentHashMap<>();

  @Inject
  public SessionWatchFeed(Database database, FdbSessionManager sessions, WatchEventChangefeed changefeed) {
    this.database = database;
    this.sessions = sessions;
    this.changefeed = changefeed;
  }

  public SessionWatchFeed(Database database) {
    this(database, null, new WatchEventChangefeed());
  }

  public synchronized Runnable connect(long sessionId, Watcher watcher, BooleanSupplier isConnected) {
    Subscription subscription = new Subscription(sessionId, watcher, isConnected);
    database.run(tr -> {
      if (sessions != null && sessions.validate(tr, sessionId, null) != KeeperException.Code.OK) {
        throw new IllegalStateException("Expired session");
      }
      changefeed.attach(tr, sessionId, subscription.owner);
      return null;
    });
    Subscription previous = subscriptions.put(sessionId, subscription);
    if (previous != null) previous.stop();
    return subscription::drain;
  }

  public byte[] epoch(Request request) {
    Subscription sub = subscriptions.get(request.sessionId);
    return sub != null && sub.watcher == request.cnxn && sub.active() ? sub.owner : null;
  }

  public void shutdown() { subscriptions.values().forEach(Subscription::stop); }

  public void submit(long sessionId, Runnable action) {
    Subscription sub = subscriptions.get(sessionId);
    if (sub == null) CompletableFuture.runAsync(action);
    else synchronized (sub.requestQueue) {
      sub.requests = sub.requests.handle((v, e) -> null).thenRunAsync(action);
    }
  }

  public void processRequest(long sessionId, Runnable action) {
    Subscription sub = subscriptions.get(sessionId);
    if (sub == null) action.run();
    else synchronized (sub) { action.run(); }
  }

  public void beforeReply(long sessionId, long zxid) {
    Subscription sub = subscriptions.get(sessionId);
    if (sub != null) {
      try { sub.deliverThrough(zxid); }
      catch (Throwable error) { sub.fail(error); throw error; }
    }
  }

  private record Armed(long zxid, CompletableFuture<Void> changed, CompletableFuture<Void> ownerChanged) {}

  private class Subscription {
    private final long sessionId;
    private final Watcher watcher;
    private final BooleanSupplier isConnected;
    private final byte[] owner = Tuple.from(UUID.randomUUID().toString()).pack();
    private final Object requestQueue = new Object();
    private volatile boolean stopped;
    private CompletableFuture<Void> requests = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> changed;
    private CompletableFuture<Void> ownerChanged;

    Subscription(long sessionId, Watcher watcher, BooleanSupplier isConnected) {
      this.sessionId = sessionId;
      this.watcher = watcher;
      this.isConnected = isConnected;
    }

    private boolean active() {
      return !stopped && subscriptions.get(sessionId) == this && isConnected.getAsBoolean();
    }

    private boolean owns(Transaction tr) {
      return active() && Arrays.equals(owner, tr.get(WatchEventChangefeed.getConnectionKey(sessionId)).join());
    }

    private synchronized void deliverThrough(long through) {
      while (active()) {
        Boolean expanded = database.run(tr -> owns(tr) ? changefeed.expand(tr, sessionId, through) : null);
        if (expanded == null) { stop(); return; }
        if (expanded) break;
      }
      while (active()) {
        List<KeyValue> events = database.run(tr -> owns(tr)
            ? tr.getRange(WatchEventChangefeed.getEventsRange(sessionId), BATCH_SIZE).asList().join() : null);
        if (events == null) { stop(); return; }
        events = new ArrayList<>(events);
        events.removeIf(event -> WatchEventChangefeed.eventZxid(event) > through);
        if (events.isEmpty()) return;
        for (KeyValue event : events) {
          if (!active()) return;
          Tuple value = Tuple.fromBytes(event.getValue());
          WatchedEvent notification = new WatchedEvent(EventType.fromInt((int) value.getLong(1)),
              KeeperState.SyncConnected, value.getString(0), WatchEventChangefeed.eventZxid(event));
          if (watcher instanceof ServerCnxn) ((ServerCnxn) watcher).process(notification, database.run(tr -> WatchEventChangefeed.eventAcls(tr, event)));
          else watcher.process(notification);
        }
        List<KeyValue> delivered = events;
        database.run(tr -> {
          if (owns(tr)) for (KeyValue event : delivered) {
            WatchEventChangefeed.recordDelivery(tr, sessionId, event);
            tr.clear(event.getKey());
          }
          return null;
        });
      }
    }

    private synchronized void drain() {
      if (!active()) { stop(); return; }
      try {
        // Capture and arm in the same committed transaction, before expanding that position.
        Armed armed = database.run(tr -> {
          if (!owns(tr)) return null;
          tr.get(WatchEventChangefeed.getTriggerKey(sessionId)).join();
          return new Armed(FdbTransactionState.current(tr),
              tr.watch(WatchEventChangefeed.getTriggerKey(sessionId)),
              tr.watch(WatchEventChangefeed.getConnectionKey(sessionId)));
        });
        if (armed == null) { stop(); return; }
        changed = armed.changed;
        ownerChanged = armed.ownerChanged;
        deliverThrough(armed.zxid);
        CompletableFuture.anyOf(changed, ownerChanged).whenCompleteAsync((v, error) -> {
          synchronized (this) {
            cancelWatches();
            if (!active()) { stop(); return; }
            if (error != null) fail(error);
            else drain();
          }
        });
      } catch (Throwable error) { fail(error); }
    }

    private void fail(Throwable error) {
      if (!stopped) LOG.error("Watch delivery failed for session {}", Long.toHexString(sessionId), error);
      stop();
    }

    private void cancelWatches() {
      if (changed != null) changed.cancel(false);
      if (ownerChanged != null) ownerChanged.cancel(false);
    }

    private synchronized void stop() {
      if (stopped) return;
      stopped = true;
      subscriptions.remove(sessionId, this);
      cancelWatches();
      if (watcher instanceof ServerCnxn) ((ServerCnxn) watcher).close(DisconnectReason.CLIENT_RECONNECT);
    }
  }
}
