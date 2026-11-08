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
  private com.ph14.fdb.zk.session.FdbSessionManager sessions;
  private com.ph14.fdb.zk.FdbAcl acls;
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
    changefeed = new WatchEventChangefeed();
    sessions = new com.ph14.fdb.zk.session.FdbSessionManager(store.database, java.time.Clock.systemUTC(),
        java.util.OptionalLong.of(500), directory("sessions"), directory("timeouts"));
    sessions.addSession(SESSION, 60000);
    acls = new com.ph14.fdb.zk.FdbAcl(new org.apache.zookeeper.server.ZooKeeperServer(),
        new com.ph14.fdb.zk.layer.FdbNodeReader(), new com.ph14.fdb.zk.layer.FdbNodeWriter());
    transport = new SessionWatchFeed(store.database, sessions, changefeed);
    connection = new RecordingConnection();
    transport.connect(SESSION, connection, () -> true).run();
  }

  private DirectorySubspace directory(String name) throws Exception {
    Constructor<DirectorySubspace> c = DirectorySubspace.class.getDeclaredConstructor(List.class, byte[].class, DirectoryLayer.class);
    c.setAccessible(true);
    return c.newInstance(List.of(name), Tuple.from("partition", name).pack(), null);
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
    expect("/a", EventType.NodeDataChanged);
    expect("/a", EventType.NodeDeleted);
    awaitEmpty();
  }

  @Test
  public void recursiveWatchesMatchDescendantsAndExcludeChildChanges() throws Exception {
    add("/a", WatcherType.PersistentRecursive);
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
      Runnable start = otherInstance.connect(SESSION, replacement, () -> true);
      store.database.run(tr -> {
        changefeed.enqueueEvent(tr, SESSION, EventType.NodeDataChanged, "/a");
        changefeed.addWatch(tr, SESSION, "/b", WatcherType.Data);
        return null;
      });
      start.run();
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
    await(() -> store.armedWatches() == 2);
    add("/a", WatcherType.Data);
    RecordingConnection replacement = new RecordingConnection();
    SessionWatchFeed otherInstance = new SessionWatchFeed(store.database);
    try {
      Runnable start = otherInstance.connect(SESSION, replacement, () -> true);
      add("/a", WatcherType.Data);
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

  @Test
  public void replyBarrierDeliversEstablishedWatchBeforeChangedReadReply() throws Exception {
    transport.processRequest(SESSION, () -> {
      add("/ordered", WatcherType.Data);
      emit("/ordered", EventType.NodeDataChanged);
      assertTrue(connection.events.isEmpty());
      long observed = store.database.run(com.ph14.fdb.zk.FdbTransactionState::current);
      transport.beforeReply(SESSION, observed);
      assertEquals("/ordered", connection.events.remove().getPath());
    });
  }

  @Test
  public void unknownCommitReturnsOriginalSequentialPathAndMultiStats() throws Exception {
    var runner = new com.ph14.fdb.zk.FdbRequestTransactions(store.database, sessions, transport, changefeed, acls);
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    transport.processRequest(SESSION, () -> {
      Request create = new Request(connection, SESSION, 30, OpCode.create, null, List.of());
      store.unknownNextCommit = true;
      var result = runner.run(create, tr -> {
        int sequence = calls.incrementAndGet();
        com.ph14.fdb.zk.FdbTransactionState.of(tr).mutate(tr);
        return com.hubspot.algebra.Result.ok(new org.apache.zookeeper.proto.CreateResponse("/candidate-" + sequence));
      });
      assertEquals("/candidate-1", ((org.apache.zookeeper.proto.CreateResponse) result.unwrapOrElseThrow()).getPath());
      assertEquals(1, calls.get());
      Request multi = new Request(connection, SESSION, 31, OpCode.multi, null, List.of());
      store.unknownNextCommit = true;
      var response = runner.run(multi, tr -> {
        calls.incrementAndGet();
        long zxid = com.ph14.fdb.zk.FdbTransactionState.of(tr).mutate(tr);
        org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
        stat.setCzxid(1); stat.setMzxid(zxid); stat.setPzxid(zxid); stat.setVersion(9);
        org.apache.zookeeper.MultiResponse record = new org.apache.zookeeper.MultiResponse();
        record.add(new org.apache.zookeeper.OpResult.CreateResult("/leader"));
        record.add(new org.apache.zookeeper.OpResult.SetDataResult(stat));
        return com.hubspot.algebra.Result.ok(record);
      });
      var results = ((org.apache.zookeeper.MultiResponse) response.unwrapOrElseThrow()).getResultList();
      assertEquals("/leader", ((org.apache.zookeeper.OpResult.CreateResult) results.get(0)).getPath());
      assertEquals(multi.zxid, ((org.apache.zookeeper.OpResult.SetDataResult) results.get(1)).getStat().getMzxid());
      assertEquals(2, calls.get());
      assertEquals(2, store.unknownRetries);
    });
  }

  @Test
  public void closingAndReplacementEpochFenceMutationTransactions() throws Exception {
    var runner = new com.ph14.fdb.zk.FdbRequestTransactions(store.database, sessions, transport, changefeed, acls);
    Request old = new Request(connection, SESSION, 42, OpCode.create, null, List.of());
    RecordingConnection replacement = new RecordingConnection();
    SessionWatchFeed other = new SessionWatchFeed(store.database, sessions, changefeed);
    try {
      other.connect(SESSION, replacement, () -> true).run();
      assertEquals(KeeperException.Code.SESSIONMOVED, runner.run(old, tr -> {
        fail("Old epoch executed mutation"); return null;
      }).unwrapErrOrElseThrow().code());
      var newer = new com.ph14.fdb.zk.FdbRequestTransactions(store.database, sessions, other, changefeed, acls);
      store.database.run(tr -> sessions.beginClose(tr, SESSION, false));
      Request request = new Request(replacement, SESSION, 43, OpCode.create, null, List.of());
      assertTrue(newer.run(request, tr -> { fail("Closing session mutated"); return null; }).isErr());
      assertFalse(sessions.touchSession(SESSION, 60000));
    } finally { other.shutdown(); }
  }

  @Test
  public void purgeCandidateRechecksRenewedDeadline() {
    assertFalse(store.database.run(tr -> sessions.beginClose(tr, SESSION, true)));
    assertTrue(sessions.touchSession(SESSION, 120000));
    assertFalse(store.database.run(tr -> sessions.beginClose(tr, SESSION, true)));
    assertTrue(store.database.run(tr -> sessions.beginClose(tr, SESSION, false)));
    assertFalse(sessions.touchSession(SESSION, 240000));
  }

  @Test
  public void crashAfterDeliveryDoesNotReplayOldPersistentEventOnAnotherInstance() throws Exception {
    add("/persistent", WatcherType.Persistent);
    transport.processRequest(SESSION, () -> {
      emit("/persistent", EventType.NodeDataChanged);
      // Simulate a committed outbox whose send was received but whose acknowledgement was lost.
      store.database.run(tr -> changefeed.expand(tr, SESSION, com.ph14.fdb.zk.FdbTransactionState.current(tr)));
    });
    RecordingConnection replacement = new RecordingConnection();
    SessionWatchFeed other = new SessionWatchFeed(store.database, sessions, changefeed);
    try {
      Runnable start = other.connect(SESSION, replacement, () -> true);
      add("/persistent", WatcherType.Persistent);
      start.run();
      other.beforeReply(SESSION, store.database.run(com.ph14.fdb.zk.FdbTransactionState::current));
      assertTrue(replacement.events.isEmpty());
      emit("/persistent", EventType.NodeDataChanged);
      assertNotNull(replacement.events.poll(2, TimeUnit.SECONDS));
      assertTrue(replacement.events.isEmpty());
    } finally { other.shutdown(); }
  }

  @Test
  public void syntheticRestoreEventsRemainDistinctAcrossTransactionsWithoutMutations() throws Exception {
    transport.processRequest(SESSION, () -> {
      store.database.run(tr -> { changefeed.enqueueEvent(tr, SESSION, EventType.NodeDeleted, "/a"); return null; });
      store.database.run(tr -> { changefeed.enqueueEvent(tr, SESSION, EventType.NodeDeleted, "/b"); return null; });
      assertEquals(2, pending().size());
    });
    transport.beforeReply(SESSION, store.database.run(com.ph14.fdb.zk.FdbTransactionState::current));
    assertEquals(2, connection.events.size());
  }

  @Test
  public void recursiveEventsDoNotLeaveDeliveryReceipts() throws Exception {
    add("/recursive", WatcherType.PersistentRecursive);
    for (int i = 0; i < 20; i++) emit("/recursive/" + i, EventType.NodeCreated);
    transport.beforeReply(SESSION, store.database.run(com.ph14.fdb.zk.FdbTransactionState::current));
    assertTrue(store.database.run(tr -> tr.getRange(WatchEventChangefeed.getDeliveredRange(SESSION)).asList().join()).isEmpty());
    store.database.run(tr -> { changefeed.enqueueEvent(tr, SESSION, EventType.NodeDeleted, "/oneshot"); return null; });
    transport.beforeReply(SESSION, store.database.run(com.ph14.fdb.zk.FdbTransactionState::current));
    assertFalse(store.database.run(tr -> tr.getRange(WatchEventChangefeed.getDeliveredRange(SESSION)).asList().join()).isEmpty());
    store.database.run(tr -> { changefeed.finishRestoration(tr, SESSION); return null; });
    assertTrue(store.database.run(tr -> tr.getRange(WatchEventChangefeed.getDeliveredRange(SESSION)).asList().join()).isEmpty());
  }

  @Test
  public void nodeOverlayKeepsUnchangedZxidsAndEarlierResponseSnapshots() {
    var node = new com.apple.foundationdb.subspace.Subspace(Tuple.from("node"));
    var unrelated = new com.apple.foundationdb.subspace.Subspace(Tuple.from("other"));
    var writer = new com.ph14.fdb.zk.layer.FdbNodeWriter();
    var reader = new com.ph14.fdb.zk.layer.FdbNodeReader();
    store.database.run(tr -> {
      writer.writeStat(tr, node, java.util.Map.of(com.ph14.fdb.zk.layer.StatKey.CZXID, 11L,
          com.ph14.fdb.zk.layer.StatKey.MZXID, 12L, com.ph14.fdb.zk.layer.StatKey.PZXID, 13L));
      writer.writeStat(tr, unrelated, java.util.Map.of(com.ph14.fdb.zk.layer.StatKey.CZXID, 21L,
          com.ph14.fdb.zk.layer.StatKey.MZXID, 22L, com.ph14.fdb.zk.layer.StatKey.PZXID, 23L));
      return null;
    });
    store.database.run(tr -> {
      writer.writeStat(tr, node, java.util.Map.of(com.ph14.fdb.zk.layer.StatKey.PZXID,
          com.ph14.fdb.zk.layer.FdbNodeWriter.ZXID_FLAG));
      var first = reader.getNodeStat(node, tr).join();
      writer.writeStat(tr, node, java.util.Map.of(com.ph14.fdb.zk.layer.StatKey.MZXID,
          com.ph14.fdb.zk.layer.FdbNodeWriter.ZXID_FLAG));
      var second = reader.getNodeStat(node, tr).join();
      assertEquals(11, second.getCzxid());
      assertEquals(first.getPzxid(), second.getMzxid());
      assertEquals(12, first.getMzxid());
      assertEquals(21, reader.getNodeStat(unrelated, tr).join().getCzxid());
      return null;
    });
  }

  @Test
  public void purgeResumesBoundedBatchesAfterUnknownCommitAndKeepsParentCounts() throws Exception {
    var writer = new com.ph14.fdb.zk.layer.FdbNodeWriter() {
      @Override
      public java.util.concurrent.CompletableFuture<Void> deleteNodeAsync(com.apple.foundationdb.Transaction tr, DirectorySubspace node) {
        tr.clear(node.range());
        return java.util.concurrent.CompletableFuture.completedFuture(null);
      }
    };
    var nodes = new java.util.HashMap<String, DirectorySubspace>();
    nodes.put("/parent", directory("parent"));
    var reader = new com.ph14.fdb.zk.layer.FdbNodeReader() {
      @Override
      public java.util.concurrent.CompletableFuture<DirectorySubspace> getNodeDirectory(com.apple.foundationdb.Transaction tr, String path) {
        return java.util.concurrent.CompletableFuture.completedFuture(nodes.get(path));
      }
    };
    var ephemerals = new com.ph14.fdb.zk.layer.ephemeral.FdbEphemeralNodeManager();
    for (int i = 0; i < 160; i++) nodes.put("/parent/e" + i, directory("e" + i));
    store.database.run(tr -> {
      writer.writeStat(tr, nodes.get("/parent"), java.util.Map.of(
          com.ph14.fdb.zk.layer.StatKey.NUM_CHILDREN, 160L, com.ph14.fdb.zk.layer.StatKey.CVERSION, 160L));
      for (int i = 0; i < 160; i++) {
        String path = "/parent/e" + i;
        writer.writeStat(tr, nodes.get(path), java.util.Map.of(com.ph14.fdb.zk.layer.StatKey.EPHEMERAL_OWNER, SESSION));
        ephemerals.addEphemeralNode(tr, path, SESSION);
      }
      sessions.beginClose(tr, SESSION, false);
      return null;
    });
    var purger = new com.ph14.fdb.zk.session.FdbSessionDataPurger(store.database, changefeed, writer, reader, ephemerals, sessions);
    store.unknownNextCommit = true;
    purger.removeExpiredSessionData(SESSION, true).get(5, TimeUnit.SECONDS);
    assertEquals(1, store.unknownRetries);
    store.database.run(tr -> {
      var parent = reader.getNodeStat(nodes.get("/parent"), tr).join();
      assertEquals(0, parent.getNumChildren());
      assertEquals(320, parent.getCversion());
      assertTrue(ephemerals.getBatch(tr, SESSION, 1).join().isEmpty());
      assertEquals(320, tr.getRange(FdbZkKeyspace.raw().range(Tuple.from("watch-log"))).asList().join().size());
      assertEquals(KeeperException.Code.SESSIONEXPIRED, sessions.validate(tr, SESSION, null));
      return null;
    });
  }

  @Test
  public void aclChecksUseRequestIdentityAndVersionedReplacementSurvivesUnknownCommit() throws Exception {
    DirectorySubspace node = directory("acl-node");
    var reader = new com.ph14.fdb.zk.layer.FdbNodeReader() {
      @Override
      public java.util.concurrent.CompletableFuture<DirectorySubspace> getNodeDirectory(com.apple.foundationdb.Transaction tr, String path) {
        return java.util.concurrent.CompletableFuture.completedFuture(node);
      }
    };
    var writer = new com.ph14.fdb.zk.layer.FdbNodeWriter();
    var policy = new com.ph14.fdb.zk.FdbAcl(new org.apache.zookeeper.server.ZooKeeperServer(), reader, writer);
    String digest = org.apache.zookeeper.server.auth.DigestAuthenticationProvider.generateDigest("owner:secret");
    var privateAcl = List.of(new ACL(org.apache.zookeeper.ZooDefs.Perms.ALL, new org.apache.zookeeper.data.Id("digest", digest)));
    store.database.run(tr -> {
      writer.writeACLs(tr, node, privateAcl);
      writer.writeStat(tr, node, java.util.Map.of(com.ph14.fdb.zk.layer.StatKey.AVERSION, 4L));
      return null;
    });
    Request denied = new Request(connection, SESSION, 70, OpCode.getData, null, List.of());
    try {
      store.database.run(tr -> { policy.authorize(denied, tr, new org.apache.zookeeper.proto.GetDataRequest("/acl", false)); return null; });
      fail("Unauthenticated read accepted");
    } catch (RuntimeException error) {
      var cause = (com.ph14.fdb.zk.FdbAcl.Failure) com.ph14.fdb.zk.ops.FdbOp.unwrap(error);
      assertEquals(KeeperException.Code.NOAUTH, cause.error.code());
    }
    Request owner = new Request(connection, SESSION, 71, OpCode.setACL, null,
        List.of(new org.apache.zookeeper.data.Id("digest", digest)));
    var command = new org.apache.zookeeper.proto.SetACLRequest("/acl", org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, 4);
    var runner = new com.ph14.fdb.zk.FdbRequestTransactions(store.database, sessions, transport, changefeed, policy);
    transport.processRequest(SESSION, () -> {
      store.unknownNextCommit = true;
      var result = runner.run(owner, command, tr -> com.hubspot.algebra.Result.ok(policy.setAcl(owner, tr, command)));
      assertEquals(5, ((org.apache.zookeeper.proto.SetACLResponse) result.unwrapOrElseThrow()).getStat().getAversion());
      assertEquals(1, store.unknownRetries);
    });
    store.database.run(tr -> {
      policy.authorize(denied, tr, new org.apache.zookeeper.proto.GetDataRequest("/acl", false));
      assertEquals(org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE, reader.readAcls(node, tr).join());
      return null;
    });
    Request badVersion = new Request(connection, SESSION, 72, OpCode.setACL, null, List.of());
    var stale = new org.apache.zookeeper.proto.SetACLRequest("/acl", privateAcl, 4);
    assertEquals(KeeperException.Code.BADVERSION, runner.run(badVersion, stale,
        tr -> com.hubspot.algebra.Result.ok(policy.setAcl(badVersion, tr, stale))).unwrapErrOrElseThrow().code());
  }

  @Test
  public void queuingAnotherRequestNeverWaitsForAnInFlightTransaction() throws Exception {
    var entered = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.CountDownLatch(1);
    var finished = new java.util.concurrent.CountDownLatch(1);
    transport.submit(SESSION, () -> transport.processRequest(SESSION, () -> {
      entered.countDown();
      try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }));
    assertTrue(entered.await(2, TimeUnit.SECONDS));
    try {
      java.util.concurrent.CompletableFuture.runAsync(() -> transport.submit(SESSION, finished::countDown)).get(1, TimeUnit.SECONDS);
      assertEquals(1, finished.getCount());
    } finally { release.countDown(); }
    assertTrue(finished.await(2, TimeUnit.SECONDS));
  }

  @Test
  public void absentExistsErrorCommitsItsWatchAndReceiptRetirementIsBounded() throws Exception {
    var runner = new com.ph14.fdb.zk.FdbRequestTransactions(store.database, sessions, transport, changefeed, acls);
    transport.processRequest(SESSION, () -> {
      Request exists = new Request(connection, SESSION, 80, OpCode.exists, null, List.of());
      assertTrue(runner.run(exists, tr -> {
        changefeed.addWatch(tr, SESSION, "/missing", WatcherType.Data);
        return com.hubspot.algebra.Result.err(new KeeperException.NoNodeException());
      }).isErr());
      assertTrue(has("/missing", WatcherType.Data));
      for (int i = 81; i < 90; i++) {
        Request create = new Request(connection, SESSION, i, OpCode.create, null, List.of());
        assertTrue(runner.run(create, tr -> {
          com.ph14.fdb.zk.FdbTransactionState.of(tr).mutate(tr);
          return com.hubspot.algebra.Result.ok(new org.apache.zookeeper.proto.CreateResponse("/created"));
        }).isOk());
      }
      assertEquals(1, store.database.run(tr -> tr.getRange(FdbZkKeyspace.raw().range(Tuple.from("request-receipt", SESSION))).asList().join()).size());
    });
  }

  @Test
  public void unknownWatchRegistrationKeepsItsOriginalReplyAheadOfTheLaterEvent() throws Exception {
    var runner = new com.ph14.fdb.zk.FdbRequestTransactions(store.database, sessions, transport, changefeed, acls);
    var replied = new AtomicBoolean();
    connection.onEvent = () -> assertTrue("Notification preceded its registration reply", replied.get());
    transport.processRequest(SESSION, () -> {
      Request request = new Request(connection, SESSION, 100, OpCode.getData, null, List.of());
      store.unknownNextCommit = true;
      store.afterUnknownCommit = () -> emit("/registration-retry", EventType.NodeDataChanged);
      var result = runner.run(request, tr -> {
        changefeed.addWatch(tr, SESSION, "/registration-retry", WatcherType.Data);
        return com.hubspot.algebra.Result.ok(new org.apache.zookeeper.proto.GetDataResponse(new byte[] {1}, new org.apache.zookeeper.data.Stat()));
      });
      assertTrue(result.isOk());
      transport.beforeReply(SESSION, request.zxid);
      assertTrue(connection.events.isEmpty());
      replied.set(true);
    });
    transport.beforeReply(SESSION, store.database.run(com.ph14.fdb.zk.FdbTransactionState::current));
    expect("/registration-retry", EventType.NodeDataChanged);
    assertEquals(1, store.unknownRetries);
  }

  private void add(String path, WatcherType type) {
    store.database.run(tr -> { changefeed.addWatch(tr, SESSION, path, type); return null; });
  }

  private void emit(String path, EventType type) {
    store.database.runAsync(tr -> changefeed.appendToChangefeed(tr, type, path)).join();
  }

  private boolean has(String path, WatcherType type) {
    while (!store.database.run(tr -> changefeed.expand(tr, SESSION,
        com.ph14.fdb.zk.FdbTransactionState.current(tr)))) { }
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
