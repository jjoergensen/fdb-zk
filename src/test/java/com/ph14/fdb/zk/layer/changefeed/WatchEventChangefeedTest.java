package com.ph14.fdb.zk.layer.changefeed;

import static org.junit.Assert.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.server.MockFdbServerCnxn;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.proto.AddWatchRequest;
import org.apache.zookeeper.proto.CheckWatchesRequest;
import org.apache.zookeeper.proto.RemoveWatchesRequest;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SetWatches2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.ph14.fdb.zk.FdbZkKeyspace;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.ops.FdbSetWatchesOp;
import com.ph14.fdb.zk.ops.FdbWatchOp;

public class WatchEventChangefeedTest {
  private static final long SESSION = 7;
  private WatchTestDatabase store;
  private WatchEventChangefeed changefeed;
  private SessionWatchFeed transport;
  private RecordingConnection connection;
  private Object previousRaw;

  @Before
  public void setUp() throws Exception {
    Field raw = FdbZkKeyspace.class.getDeclaredField("raw");
    raw.setAccessible(true);
    previousRaw = raw.get(null);
    Constructor<DirectorySubspace> constructor = DirectorySubspace.class.getDeclaredConstructor(List.class, byte[].class, DirectoryLayer.class);
    constructor.setAccessible(true);
    raw.set(null, constructor.newInstance(Collections.singletonList("raw"), Tuple.from("partition", "raw").pack(), null));
    store = new WatchTestDatabase();
    changefeed = new WatchEventChangefeed() {
      @Override
      byte[] packWithVersionstamp(Tuple tuple, byte[] prefix) {
        // The native API selection in FDB's incomplete-stamp encoder is unavailable in these tests.
        List<Object> items = new ArrayList<>(tuple.getItems());
        int offset = 0;
        for (int i = 0; i < items.size(); i++) {
          if (items.get(i) instanceof Versionstamp) {
            offset = (prefix == null ? 0 : prefix.length) + Tuple.fromList(items.subList(0, i)).pack().length + 1;
            items.set(i, Versionstamp.complete(new byte[10]));
          }
        }
        byte[] packed = Tuple.fromList(items).pack(prefix);
        return ByteBuffer.allocate(packed.length + 4).order(ByteOrder.LITTLE_ENDIAN).put(packed).putInt(offset).array();
      }
    };
    transport = new SessionWatchFeed(store.database);
    connection = new RecordingConnection();
  }

  @After
  public void tearDown() throws Exception {
    transport.shutdown();
    await(() -> store.armedWatches() == 0);
    Field raw = FdbZkKeyspace.class.getDeclaredField("raw");
    raw.setAccessible(true);
    raw.set(null, previousRaw);
  }

  @Test
  public void drainsConcurrentWatchesInReverseOrderAndRearms() throws Exception {
    transport.connect(SESSION, connection, () -> true).run();
    for (int i = 0; i < 6; i++) add("/p" + i, WatcherType.Children);
    for (int i = 5; i >= 0; i--) {
      emit("/p" + i, EventType.NodeChildrenChanged);
      expect("/p" + i, EventType.NodeChildrenChanged);
      assertFalse(has("/p" + i, WatcherType.Children));
    }
    add("/p0", WatcherType.Children);
    emit("/p0", EventType.NodeChildrenChanged);
    expect("/p0", EventType.NodeChildrenChanged);
    awaitEmpty();
    assertTrue(connection.events.isEmpty());
  }

  @Test
  public void retainsDistinctPathsAndMutationsWithinOneCommit() throws Exception {
    add("/a", WatcherType.Data);
    add("/b", WatcherType.Data);
    add("/persistent", WatcherType.Persistent);
    store.database.run(tr -> {
      changefeed.appendToChangefeed(tr, EventType.NodeDataChanged, "/b").join();
      changefeed.appendToChangefeed(tr, EventType.NodeDataChanged, "/a").join();
      changefeed.appendToChangefeed(tr, EventType.NodeDataChanged, "/persistent").join();
      changefeed.appendToChangefeed(tr, EventType.NodeDataChanged, "/persistent").join();
      return null;
    });
    transport.connect(SESSION, connection, () -> true).run();
    expect("/b", EventType.NodeDataChanged);
    expect("/a", EventType.NodeDataChanged);
    expect("/persistent", EventType.NodeDataChanged);
    expect("/persistent", EventType.NodeDataChanged);
    awaitEmpty();
  }

  @Test
  public void consumesCompleteOneShotRegistrationsAndDeduplicatesOnlyEquivalentEvents() throws Exception {
    add("/a", WatcherType.Data);
    add("/a", WatcherType.Children);
    add("/a", WatcherType.Persistent);
    add("/", WatcherType.PersistentRecursive);
    transport.connect(SESSION, connection, () -> true).run();
    emit("/a", EventType.NodeDeleted);
    expect("/a", EventType.NodeDeleted);
    awaitEmpty();
    assertFalse(has("/a", WatcherType.Data));
    assertFalse(has("/a", WatcherType.Children));
    assertTrue(has("/a", WatcherType.Persistent));
    assertTrue(connection.events.isEmpty());
    emit("/a", EventType.NodeCreated);
    expect("/a", EventType.NodeCreated);
    awaitEmpty();
    assertTrue(connection.events.isEmpty());
  }

  @Test
  public void dataAndChildWatchesRemainIndependentUntilDeletion() throws Exception {
    add("/a", WatcherType.Data);
    add("/a", WatcherType.Children);
    emit("/a", EventType.NodeDataChanged);
    assertFalse(has("/a", WatcherType.Data));
    assertTrue(has("/a", WatcherType.Children));
    emit("/a", EventType.NodeDeleted);
    transport.connect(SESSION, connection, () -> true).run();
    expect("/a", EventType.NodeDataChanged);
    expect("/a", EventType.NodeDeleted);
    awaitEmpty();
  }

  @Test
  public void recursiveWatchesMatchDescendantsAndExcludeChildChanges() throws Exception {
    add("/a", WatcherType.PersistentRecursive);
    transport.connect(SESSION, connection, () -> true).run();
    emit("/a/b/c", EventType.NodeCreated);
    expect("/a/b/c", EventType.NodeCreated);
    emit("/a", EventType.NodeDataChanged);
    expect("/a", EventType.NodeDataChanged);
    emit("/a", EventType.NodeChildrenChanged);
    emit("/a/b", EventType.NodeChildrenChanged);
    emit("/ab", EventType.NodeCreated);
    emit("/", EventType.NodeDataChanged);
    awaitEmpty();
    assertTrue(connection.events.isEmpty());
    assertTrue(has("/a", WatcherType.PersistentRecursive));
  }

  @Test
  public void removalsAndCleanupIncludeUntouchedRegistrations() {
    add("/a", WatcherType.Data);
    add("/a", WatcherType.Children);
    add("/a", WatcherType.Persistent);
    add("/deep", WatcherType.PersistentRecursive);
    assertTrue(remove("/a", WatcherType.Data));
    assertFalse(remove("/a", WatcherType.Data));
    assertTrue(has("/a", WatcherType.Children));
    assertTrue(remove("/a", WatcherType.Any));
    assertFalse(has("/a", WatcherType.Any));
    store.database.runAsync(tr -> changefeed.clearAllWatchesForSession(tr, SESSION)).join();
    assertFalse(has("/deep", WatcherType.Any));
    emit("/deep/x", EventType.NodeCreated);
    assertTrue(pending().isEmpty());
  }

  @Test
  public void abortedChangesDoNotConsumeWatchesOrSendNotifications() throws Exception {
    add("/a", WatcherType.Data);
    transport.connect(SESSION, connection, () -> true).run();
    await(() -> store.armedWatches() == 2);
    WatchTestDatabase.Tx aborted = store.transaction();
    changefeed.appendToChangefeed(aborted.transaction, EventType.NodeDataChanged, "/a").join();
    assertTrue(pending().isEmpty());
    assertTrue(connection.events.isEmpty());
    assertFalse(aborted.transaction.getVersionstamp().isDone());
    aborted.transaction.cancel();
    assertTrue(has("/a", WatcherType.Data));
    emit("/a", EventType.NodeDataChanged);
    expect("/a", EventType.NodeDataChanged);
    awaitEmpty();
  }

  @Test
  public void reconnectToAnotherInstanceDeliversPendingAndFutureWatches() throws Exception {
    AtomicBoolean connected = new AtomicBoolean(true);
    transport.connect(SESSION, connection, connected::get).run();
    await(() -> store.armedWatches() == 2);
    add("/a", WatcherType.Data);
    add("/b", WatcherType.Data);
    connected.set(false);
    emit("/a", EventType.NodeDataChanged);
    RecordingConnection replacement = new RecordingConnection();
    SessionWatchFeed otherInstance = new SessionWatchFeed(store.database);
    try {
      otherInstance.connect(SESSION, replacement, () -> true).run();
      WatchedEvent pending = replacement.events.poll(2, TimeUnit.SECONDS);
      assertNotNull(pending);
      assertEquals("/a", pending.getPath());
      emit("/b", EventType.NodeDataChanged);
      WatchedEvent future = replacement.events.poll(2, TimeUnit.SECONDS);
      assertNotNull(future);
      assertEquals("/b", future.getPath());
      awaitEmpty();
      assertTrue(connection.events.isEmpty());
      assertTrue(replacement.events.isEmpty());
    } finally {
      otherInstance.shutdown();
    }
  }

  @Test
  public void activeConnectionIsFencedWhenAnotherInstanceClaimsTheSession() throws Exception {
    transport.connect(SESSION, connection, () -> true).run();
    await(() -> store.armedWatches() == 2);
    add("/a", WatcherType.Data);
    RecordingConnection replacement = new RecordingConnection();
    SessionWatchFeed otherInstance = new SessionWatchFeed(store.database);
    try {
      Runnable start = otherInstance.connect(SESSION, replacement, () -> true);
      emit("/a", EventType.NodeDataChanged);
      assertTrue(replacement.events.isEmpty());
      start.run();
      assertNotNull(replacement.events.poll(2, TimeUnit.SECONDS));
      awaitEmpty();
      assertTrue(connection.events.isEmpty());
    } finally {
      otherInstance.shutdown();
    }
  }

  @Test
  public void drainsMultipleBatchesWithoutWaitingForAnotherMutation() throws Exception {
    add("/p", WatcherType.Persistent);
    store.database.run(tr -> {
      for (int i = 0; i < 260; i++) changefeed.appendToChangefeed(tr, EventType.NodeDataChanged, "/p").join();
      return null;
    });
    transport.connect(SESSION, connection, () -> true).run();
    for (int i = 0; i < 260; i++) expect("/p", EventType.NodeDataChanged);
    awaitEmpty();
    assertTrue(connection.events.isEmpty());
  }

  @Test
  public void restorationDoesNotDuplicatePendingOrDeliveredNotifications() throws Exception {
    add("/a", WatcherType.Data);
    emit("/a", EventType.NodeDataChanged);
    FdbSetWatchesOp restore = new FdbSetWatchesOp(null, new FdbWatchManager(changefeed));
    Request request = new Request(connection, SESSION, 1, OpCode.setWatches, null, Collections.emptyList());
    SetWatches watches = new SetWatches(0, List.of("/a"), List.of(), List.of());
    assertTrue(store.database.runAsync(tr -> restore.execute(request, tr, watches)).join().isOk());
    transport.connect(SESSION, connection, () -> true).run();
    expect("/a", EventType.NodeDataChanged);
    awaitEmpty();
    assertTrue(store.database.runAsync(tr -> restore.execute(request, tr, watches)).join().isOk());
    assertFalse(has("/a", WatcherType.Data));
    assertTrue(pending().isEmpty());
    assertTrue(connection.events.isEmpty());
  }

  @Test
  public void restoresBothPersistentListsAndHonorsRemovalTypes() throws Exception {
    FdbWatchManager manager = new FdbWatchManager(changefeed);
    FdbSetWatchesOp restore = new FdbSetWatchesOp(null, manager);
    FdbWatchOp watches = new FdbWatchOp(manager);
    Request request = new Request(connection, SESSION, 1, OpCode.setWatches2, null, Collections.emptyList());
    SetWatches2 lists = new SetWatches2(0, List.of(), List.of(), List.of(), List.of("/p"), List.of("/r"));
    assertTrue(store.database.runAsync(tr -> restore.execute(request, tr, lists)).join().isOk());
    assertTrue(has("/p", WatcherType.Persistent));
    assertTrue(has("/r", WatcherType.PersistentRecursive));
    transport.connect(SESSION, connection, () -> true).run();
    emit("/p", EventType.NodeChildrenChanged);
    expect("/p", EventType.NodeChildrenChanged);
    emit("/r/a", EventType.NodeDataChanged);
    expect("/r/a", EventType.NodeDataChanged);
    awaitEmpty();
    assertEquals(KeeperException.Code.NOWATCHER, store.database.runAsync(tr -> watches.execute(request, tr,
        new CheckWatchesRequest("/p", WatcherType.Data.getIntValue()))).join().unwrapErrOrElseThrow().code());
    assertTrue(store.database.runAsync(tr -> watches.execute(request, tr,
        new RemoveWatchesRequest("/p", WatcherType.Persistent.getIntValue()))).join().isOk());
    assertEquals(KeeperException.Code.NOWATCHER, store.database.runAsync(tr -> watches.execute(request, tr,
        new RemoveWatchesRequest("/p", WatcherType.Persistent.getIntValue()))).join().unwrapErrOrElseThrow().code());
    assertTrue(store.database.runAsync(tr -> watches.execute(request, tr,
        new CheckWatchesRequest("/r", WatcherType.PersistentRecursive.getIntValue()))).join().isOk());
  }

  @Test
  public void addWatchAcceptsMissingPathsAndRejectsInvalidModesAndPaths() {
    FdbWatchOp watches = new FdbWatchOp(new FdbWatchManager(changefeed));
    Request request = new Request(connection, SESSION, 1, OpCode.addWatch, null, Collections.emptyList());
    assertTrue(store.database.runAsync(tr -> watches.execute(request, tr,
        new AddWatchRequest("/missing", AddWatchMode.PERSISTENT.getMode()))).join().isOk());
    assertTrue(store.database.runAsync(tr -> watches.execute(request, tr,
        new AddWatchRequest("/missing", AddWatchMode.PERSISTENT_RECURSIVE.getMode()))).join().isOk());
    assertEquals(KeeperException.Code.BADARGUMENTS, store.database.runAsync(tr -> watches.execute(request, tr,
        new AddWatchRequest("/bad", 999))).join().unwrapErrOrElseThrow().code());
    assertEquals(KeeperException.Code.BADARGUMENTS, store.database.runAsync(tr -> watches.execute(request, tr,
        new AddWatchRequest("bad", AddWatchMode.PERSISTENT.getMode()))).join().unwrapErrOrElseThrow().code());
    assertEquals(KeeperException.Code.BADARGUMENTS, store.database.runAsync(tr -> watches.execute(request, tr,
        new CheckWatchesRequest("/missing", 999))).join().unwrapErrOrElseThrow().code());
    assertEquals(KeeperException.Code.BADARGUMENTS, store.database.runAsync(tr -> watches.execute(request, tr,
        new RemoveWatchesRequest("/missing", 999))).join().unwrapErrOrElseThrow().code());
  }

  @Test
  public void notificationsFollowTheRegistrationReply() throws Exception {
    transport.connect(SESSION, connection, () -> true).run();
    await(() -> store.armedWatches() == 2);
    transport.processRequest(SESSION, () -> {
      add("/a", WatcherType.Data);
      emit("/a", EventType.NodeDataChanged);
      assertTrue(connection.events.isEmpty());
    });
    expect("/a", EventType.NodeDataChanged);
    awaitEmpty();
  }

  @Test
  public void acknowledgementRetriesDoNotRepeatNetworkSends() throws Exception {
    connection.onEvent = () -> store.retryNextWrite = true;
    add("/a", WatcherType.Data);
    transport.connect(SESSION, connection, () -> true).run();
    emit("/a", EventType.NodeDataChanged);
    expect("/a", EventType.NodeDataChanged);
    awaitEmpty();
    assertEquals(1, store.retries);
    assertTrue(connection.events.isEmpty());
  }

  @Test
  public void reregistrationBeforeAcknowledgementDoesNotInheritADeliveryReceipt() throws Exception {
    AtomicBoolean rearm = new AtomicBoolean(true);
    connection.onEvent = () -> {
      if (rearm.getAndSet(false)) add("/a", WatcherType.Data);
    };
    add("/a", WatcherType.Data);
    transport.connect(SESSION, connection, () -> true).run();
    emit("/a", EventType.NodeDataChanged);
    expect("/a", EventType.NodeDataChanged);
    awaitEmpty();
    assertTrue(has("/a", WatcherType.Data));
    assertFalse(store.database.runAsync(tr -> changefeed.wasNotified(tr, SESSION, "/a", WatcherType.Data)).join());
    emit("/a", EventType.NodeDataChanged);
    expect("/a", EventType.NodeDataChanged);
    awaitEmpty();
    assertFalse(has("/a", WatcherType.Data));
    assertTrue(connection.events.isEmpty());
  }

  private void add(String path, WatcherType type) {
    store.database.run(tr -> { changefeed.addWatch(tr, SESSION, path, type); return null; });
  }

  private void emit(String path, EventType type) {
    store.database.runAsync(tr -> changefeed.appendToChangefeed(tr, type, path)).join();
  }

  private boolean has(String path, WatcherType type) {
    return store.database.runAsync(tr -> changefeed.checkWatch(tr, SESSION, path, type, false)).join();
  }

  private boolean remove(String path, WatcherType type) {
    return store.database.runAsync(tr -> changefeed.checkWatch(tr, SESSION, path, type, true)).join();
  }

  private List<com.apple.foundationdb.KeyValue> pending() {
    return store.database.runAsync(tr -> tr.getRange(WatchEventChangefeed.getEventsRange(SESSION)).asList()).join();
  }

  private void expect(String path, EventType type) throws Exception {
    WatchedEvent event = connection.events.poll(2, TimeUnit.SECONDS);
    assertNotNull("Missing " + type + " on " + path, event);
    assertEquals(type, event.getType());
    assertEquals(path, event.getPath());
  }

  private void awaitEmpty() throws Exception {
    await(() -> pending().isEmpty());
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(1);
    assertTrue(condition.getAsBoolean());
  }

  private static class RecordingConnection extends MockFdbServerCnxn {
    final BlockingQueue<WatchedEvent> events = new LinkedBlockingQueue<>();
    Runnable onEvent = () -> {};

    @Override
    public void process(WatchedEvent event) {
      throw new AssertionError("Use the ServerCnxn event/ACL transport");
    }

    @Override
    public void process(WatchedEvent event, List<ACL> acl) {
      onEvent.run();
      events.add(event);
    }
  }
}
