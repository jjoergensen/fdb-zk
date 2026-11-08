package com.ph14.fdb.zk.layer.changefeed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.inject.Singleton;
import com.ph14.fdb.zk.FdbTransactionState;
import com.ph14.fdb.zk.FdbZkKeyspace;

@Singleton
public class WatchEventChangefeed {
  private static final String REGISTRATIONS = "fdb-zk-watch-session";
  private static final String EVENTS = "fdb-zk-watch-cf";
  private static final String LOG = "watch-log";
  private static final String PENDING = "watch-pending";
  private static final String DELIVERED = "fdb-zk-watch-delivered";
  private static final int BATCH_SIZE = 64;

  private static byte[] key(Object... values) {
    return FdbZkKeyspace.raw().pack(Tuple.from(values));
  }

  private static Range range(Object... values) {
    return FdbZkKeyspace.raw().range(Tuple.from(values));
  }

  public void addWatch(Transaction tr, long session, String path, WatcherType type) {
    byte[] epoch = epoch(tr, session);
    byte[] registration = registrationKey(session, path, type);
    byte[] existing = tr.get(registration).join();
    if (existing == null || !Arrays.equals(Tuple.fromBytes(existing).getBytes(0), epoch)) {
      tr.set(registration, Tuple.from(epoch, FdbTransactionState.of(tr).position()).pack());
    }
    tr.clear(pendingKey(session, path, type));
    tr.clear(key(DELIVERED, session, path, type.getIntValue()));
  }

  public CompletableFuture<Boolean> wasNotified(Transaction tr, long session, String path, WatcherType type) {
    return tr.get(pendingKey(session, path, type)).thenCompose(pending -> pending != null
        ? CompletableFuture.completedFuture(true)
        : tr.get(key(DELIVERED, session, path, type.getIntValue())).thenApply(value -> value != null));
  }

  public CompletableFuture<Boolean> checkWatch(Transaction tr, long session, String path, WatcherType type, boolean remove) {
    boolean found = false;
    for (WatcherType registered : new WatcherType[] {WatcherType.Data, WatcherType.Children,
        WatcherType.Persistent, WatcherType.PersistentRecursive}) {
      if (type != WatcherType.Any && type != registered) continue;
      byte[] registration = registrationKey(session, path, registered);
      byte[] value = tr.get(registration).join();
      if (value != null && Arrays.equals(Tuple.fromBytes(value).getBytes(0), epoch(tr, session))) {
        found = true;
        if (remove) tr.clear(registration);
      }
    }
    return CompletableFuture.completedFuture(found);
  }

  public CompletableFuture<Void> appendToChangefeed(Transaction tr, EventType type, String path) {
    long zxid = FdbTransactionState.of(tr).mutate(tr);
    long ordinal = FdbTransactionState.of(tr).nextEventOrdinal();
    List<org.apache.zookeeper.data.ACL> acls = FdbTransactionState.of(tr).eventAcls.get(path);
    if (acls != null) {
      List<Tuple> encoded = new ArrayList<>();
      for (org.apache.zookeeper.data.ACL acl : acls) encoded.add(Tuple.from(acl.getPerms(), acl.getId().getScheme(), acl.getId().getId()));
      byte[] bytes = Tuple.fromList(encoded).pack();
      if (bytes.length == 0) tr.set(key("watch-event-acl", zxid, ordinal, 0), bytes);
      for (int offset = 0, part = 0; offset < bytes.length; offset += 80000, part++) {
        tr.set(key("watch-event-acl", zxid, ordinal, part), Arrays.copyOfRange(bytes, offset, Math.min(bytes.length, offset + 80000)));
      }
    }
    tr.set(key(LOG, zxid, ordinal), Tuple.from(path, type.getIntValue(), System.currentTimeMillis(), acls != null).pack());
    tr.set(getTriggerKey(0), Tuple.from(zxid, ordinal).pack());
    return CompletableFuture.completedFuture(null);
  }

  public void enqueueEvent(Transaction tr, long session, EventType type, String path) {
    byte[] sequenceKey = key("watch-restore-sequence", session);
    byte[] previous = tr.get(sequenceKey).join();
    long ordinal = previous == null ? 1 : Tuple.fromBytes(previous).getLong(0) + 1;
    tr.set(sequenceKey, Tuple.from(ordinal).pack());
    List<Integer> consumed = new ArrayList<>();
    for (WatcherType wt : new WatcherType[] {WatcherType.Data, WatcherType.Children}) {
      if (matches(wt, type)) consumed.add(wt.getIntValue());
    }
    enqueue(tr, session, FdbTransactionState.of(tr).position(), -ordinal, path, type, consumed, Tuple.from());
  }

  private void enqueue(Transaction tr, long session, long zxid, long ordinal, String path,
                       EventType type, List<Integer> consumed, Tuple aclReference) {
    byte[] eventKey = key(EVENTS, session, zxid, ordinal);
    tr.set(eventKey, Tuple.from(path, type.getIntValue(), Tuple.fromList(consumed), epoch(tr, session), aclReference).pack());
    for (int wt : consumed) tr.set(pendingKey(session, path, WatcherType.fromInt(wt)), eventKey);
  }

  // Expand only this session's registrations; a deletion never fans out inside its transaction.
  public boolean expand(Transaction tr, long session, long through) {
    byte[] cursorKey = key("watch-cursor", session);
    byte[] cursor = tr.get(cursorKey).join();
    byte[] floor = tr.get(key("watch-log-floor")).join();
    if (cursor != null && floor != null && Arrays.compareUnsigned(cursor, floor) < 0) {
      throw new IllegalStateException("Watch consumer fell behind retained history; reconnect required");
    }
    byte[] begin = cursor == null ? range(LOG).begin : ByteArrayUtil.join(cursor, new byte[] {0});
    byte[] end = key(LOG, through + 1);
    if (Arrays.compareUnsigned(begin, end) >= 0) return true;
    List<KeyValue> changes = tr.snapshot().getRange(new Range(begin, end), BATCH_SIZE).asList().join();
    byte[] epoch = epoch(tr, session);
    for (KeyValue change : changes) {
      Tuple position = FdbZkKeyspace.raw().unpack(change.getKey());
      long zxid = position.getLong(1);
      Tuple value = Tuple.fromBytes(change.getValue());
      String path = value.getString(0);
      EventType type = EventType.fromInt((int) value.getLong(1));
      boolean notify = false;
      List<Integer> consumed = new ArrayList<>();
      for (WatcherType wt : new WatcherType[] {WatcherType.Data, WatcherType.Children,
          WatcherType.Persistent, WatcherType.PersistentRecursive}) {
        String watched = path;
        while (true) {
          byte[] registration = registrationKey(session, watched, wt);
          byte[] bytes = tr.get(registration).join();
          if (bytes != null) {
            Tuple reg = Tuple.fromBytes(bytes);
            if (Arrays.equals(epoch, reg.getBytes(0)) && zxid > reg.getLong(1) && matches(wt, type)) {
              notify = true;
              if (wt == WatcherType.Data || wt == WatcherType.Children) {
                tr.clear(registration);
                consumed.add(wt.getIntValue());
              }
            }
          }
          if (wt != WatcherType.PersistentRecursive || watched.equals("/")) break;
          int slash = watched.lastIndexOf('/');
          watched = slash == 0 ? "/" : watched.substring(0, slash);
        }
      }
      if (notify) enqueue(tr, session, zxid, position.getLong(2), path, type, consumed,
          value.getBoolean(3) ? Tuple.from(zxid, position.getLong(2)) : Tuple.from());
      tr.set(cursorKey, change.getKey());
    }
    if (changes.size() < BATCH_SIZE) {
      tr.set(cursorKey, key(LOG, through, Long.MAX_VALUE));
      return true;
    }
    return false;
  }

  public boolean pruneLog(Transaction tr, long now) {
    byte[] floor = tr.get(key("watch-log-floor")).join();
    byte[] begin = floor == null ? range(LOG).begin : ByteArrayUtil.join(floor, new byte[] {0});
    int removed = 0;
    for (KeyValue event : tr.snapshot().getRange(new Range(begin, range(LOG).end), BATCH_SIZE).asList().join()) {
      if (Tuple.fromBytes(event.getValue()).getLong(2) >= now - 600_000L) break;
      tr.clear(event.getKey());
      Tuple position = FdbZkKeyspace.raw().unpack(event.getKey());
      tr.clear(range("watch-event-acl", position.getLong(1), position.getLong(2)));
      tr.set(key("watch-log-floor"), event.getKey());
      removed++;
    }
    return removed == BATCH_SIZE;
  }

  public void attach(Transaction tr, long session, byte[] owner) {
    tr.set(getConnectionKey(session), owner);
    tr.clear(getEventsRange(session));
    tr.clear(range(PENDING, session));
    tr.clear(getDeliveredRange(session));
    tr.set(key("watch-restoring", session), owner);
    tr.set(key("watch-cursor", session), key(LOG, FdbTransactionState.current(tr), Long.MAX_VALUE));
  }

  public void finishRestoration(Transaction tr, long session) {
    tr.clear(key("watch-restoring", session));
    tr.clear(getDeliveredRange(session));
  }

  public CompletableFuture<Void> clearAllWatchesForSession(Transaction tr, long session) {
    tr.clear(range(REGISTRATIONS, session));
    tr.clear(getEventsRange(session));
    tr.clear(range(PENDING, session));
    tr.clear(getDeliveredRange(session));
    tr.clear(getConnectionKey(session));
    tr.clear(key("watch-cursor", session));
    tr.clear(key("watch-restoring", session));
    tr.clear(key("watch-restore-sequence", session));
    return CompletableFuture.completedFuture(null);
  }

  private static boolean matches(WatcherType wt, EventType type) {
    switch (wt) {
      case Data: return type == EventType.NodeCreated || type == EventType.NodeDataChanged || type == EventType.NodeDeleted;
      case Children: return type == EventType.NodeChildrenChanged || type == EventType.NodeDeleted;
      case Persistent: return true;
      case PersistentRecursive: return type != EventType.NodeChildrenChanged;
      default: return false;
    }
  }

  static List<org.apache.zookeeper.data.ACL> eventAcls(Transaction tr, KeyValue event) {
    Tuple reference = Tuple.fromBytes(event.getValue()).getNestedTuple(4);
    if (reference.isEmpty()) return null;
    List<KeyValue> parts = tr.getRange(range("watch-event-acl", reference.getLong(0), reference.getLong(1))).asList().join();
    if (parts.isEmpty()) throw new IllegalStateException("Watch ACL history expired; reconnect required");
    byte[] bytes = ByteArrayUtil.join(parts.stream().map(KeyValue::getValue).toArray(byte[][]::new));
    List<org.apache.zookeeper.data.ACL> acls = new ArrayList<>();
    Tuple encoded = Tuple.fromBytes(bytes);
    for (int i = 0; i < encoded.size(); i++) {
      Tuple acl = encoded.getNestedTuple(i);
      acls.add(new org.apache.zookeeper.data.ACL((int) acl.getLong(0), new org.apache.zookeeper.data.Id(acl.getString(1), acl.getString(2))));
    }
    return acls;
  }

  static long eventZxid(KeyValue event) {
    return FdbZkKeyspace.raw().unpack(event.getKey()).getLong(2);
  }

  static Range getEventsRange(long session) { return range(EVENTS, session); }
  static byte[] getTriggerKey(long session) { return key("watch-log-trigger"); }
  public static byte[] getConnectionKey(long session) {
    return com.ph14.fdb.zk.session.FdbSessionManager.connectionKey(session);
  }
  static Range getDeliveredRange(long session) { return range(DELIVERED, session); }
  static byte[] epoch(Transaction tr, long session) {
    byte[] value = tr.get(getConnectionKey(session)).join();
    return value == null ? new byte[0] : value;
  }
  private static byte[] registrationKey(long session, String path, WatcherType wt) {
    return key(REGISTRATIONS, session, path, wt.getIntValue());
  }
  private static byte[] pendingKey(long session, String path, WatcherType wt) {
    return key(PENDING, session, path, wt.getIntValue());
  }

  static void recordDelivery(Transaction tr, long session, KeyValue event) {
    Tuple value = Tuple.fromBytes(event.getValue());
    for (Object item : value.getNestedTuple(2).getItems()) {
      WatcherType wt = WatcherType.fromInt(((Number) item).intValue());
      byte[] pending = pendingKey(session, value.getString(0), wt);
      if (Arrays.equals(tr.get(pending).join(), event.getKey())) {
        tr.clear(pending);
        if (tr.get(key("watch-restoring", session)).join() != null) {
          tr.set(key(DELIVERED, session, value.getString(0), wt.getIntValue()), new byte[0]);
        }
      }
    }
  }
}
