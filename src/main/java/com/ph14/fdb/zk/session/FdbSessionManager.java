package com.ph14.fdb.zk.session;

import java.io.PrintWriter;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.SessionTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class FdbSessionManager implements SessionTracker {

  private static final Logger LOG = LoggerFactory.getLogger(FdbSessionManager.class);

  private static final byte[] EMPTY_VALUE = new byte[0];

  static final List<String> SESSION_DIRECTORY = Collections.singletonList("fdb-zk-sessions-by-id");
  static final List<String> SESSION_TIMEOUT_DIRECTORY = Collections.singletonList("fdb-zk-sessions-by-timeout");

  private final Database fdb;
  private final DirectorySubspace sessionsById;
  private final DirectorySubspace sessionsByTimeout;
  private final Clock clock;
  private final long serverTickMillis;

  @Inject
  public FdbSessionManager(Database fdb,
                           Clock clock,
                           @Named("serverTickMillis") OptionalLong serverTickMillis) {
    this(fdb, clock, serverTickMillis,
        fdb.run(tr -> com.ph14.fdb.zk.FdbZkKeyspace.dir().createOrOpen(tr, SESSION_DIRECTORY).join()),
        fdb.run(tr -> com.ph14.fdb.zk.FdbZkKeyspace.dir().createOrOpen(tr, SESSION_TIMEOUT_DIRECTORY).join()));
  }

  @VisibleForTesting
  public FdbSessionManager(Database fdb, Clock clock, OptionalLong serverTickMillis,
                           DirectorySubspace sessionsById, DirectorySubspace sessionsByTimeout) {
    this.fdb = fdb;
    this.sessionsById = sessionsById;
    this.sessionsByTimeout = sessionsByTimeout;
    this.clock = clock;
    this.serverTickMillis = serverTickMillis.orElse(500L);
  }

  @Override
  public long createSession(int sessionTimeout) {
    return fdb.run(tr -> {
      long sessionExpirationTimestamp = calculateSessionExpirationTimestamp(clock.millis(), sessionTimeout);

      // magic incantation for `FIRST_IN_BATCH` transaction option.
      // forces this transaction to get batch id == 0, so any competing
      // sessions are guaranteed to be unique by the transaction version
      // component of versionstamps alone
      tr.options().getOptionConsumer().setOption(710, null);

      // session id --> (next expiration, timeoutMs)
      tr.mutate(
          MutationType.SET_VERSIONSTAMPED_KEY,
          sessionsById.packWithVersionstamp(Tuple.from(Versionstamp.incomplete())),
          getSessionByIdValue(sessionExpirationTimestamp, sessionTimeout));

      // next expiration --> session ids
      tr.mutate(
          MutationType.SET_VERSIONSTAMPED_KEY,
          getIncompleteSessionByTimestampKey(sessionExpirationTimestamp),
          EMPTY_VALUE);

      return tr.getVersionstamp();
    })
        .thenApply(Versionstamp::complete)
        .thenApply(Versionstamp::getTransactionVersion)
        .thenApply(Longs::fromByteArray)
        .join();
  }

  // ZooKeeper 3.6+ split addSession into track/commit; every session here is global and lives in FDB.
  @Override
  public boolean trackSession(long sessionId, int sessionTimeout) {
    return touchSession(sessionId, sessionTimeout);
  }

  @Override
  public boolean commitSession(long sessionId, int sessionTimeout) {
    return touchSession(sessionId, sessionTimeout);
  }

  @Override
  public boolean isTrackingSession(long sessionId) {
    return true;
  }

  @Override
  public void checkGlobalSession(long sessionId, Object owner) throws SessionExpiredException, SessionMovedException {
    checkSession(sessionId, owner);
  }

  @Override
  public java.util.Map<Long, java.util.Set<Long>> getSessionExpiryMap() {
    return java.util.Collections.emptyMap();
  }

  @Override
  public long getLocalSessionCount() {
    return 0;
  }

  @Override
  public boolean isLocalSessionsEnabled() {
    return false;
  }

  @Override
  public java.util.Set<Long> globalSessions() {
    return java.util.Collections.emptySet();
  }

  @Override
  public java.util.Set<Long> localSessions() {
    return java.util.Collections.emptySet();
  }

  public void addSession(long sessionId, int sessionTimeout) {
    long sessionExpirationTimestamp = calculateSessionExpirationTimestamp(clock.millis(), sessionTimeout);

    fdb.run(tr -> {
      byte[] sessionByIdKey = getSessionByIdKey(sessionId);

      tr.get(sessionByIdKey).thenAccept(currentSession -> {
        if (currentSession == null) {
          // session id --> next expiration
          tr.set(
              sessionByIdKey,
              getSessionByIdValue(sessionExpirationTimestamp, sessionTimeout));

          // next expiration --> session ids
          tr.set(
              getSessionByTimestampKey(sessionId, sessionExpirationTimestamp),
              EMPTY_VALUE);
        }
      }).join();

      return null;
    });

    touchSession(sessionId, sessionTimeout);
  }

  @Override
  public boolean touchSession(long sessionId, int sessionTimeout) {
    return fdb.run(tr -> {
      byte[] value = tr.get(getSessionByIdKey(sessionId)).join();
      if (value == null || tr.get(closingKey(sessionId)).join() != null) {
        return false;
      }
      Tuple current = Tuple.fromBytes(value);
      long expiration = calculateSessionExpirationTimestamp(clock.millis(), sessionTimeout);
      if (expiration > current.getLong(0)) {
        tr.clear(getSessionByTimestampKey(sessionId, current.getLong(0)));
        tr.set(getSessionByIdKey(sessionId), getSessionByIdValue(expiration, sessionTimeout));
        tr.set(getSessionByTimestampKey(sessionId, expiration), EMPTY_VALUE);
      }
      return true;
    });
  }

  @Override
  public void setSessionClosing(long sessionId) {
    fdb.run(tr -> { beginClose(tr, sessionId, false); return null; });
  }

  @Override
  public void removeSession(long sessionId) {
    fdb.runAsync(tr -> removeSessionAsync(tr, sessionId)).join();
  }

  public CompletableFuture<Void> removeSessionAsync(Transaction transaction, long sessionId) {
    byte[] sessionByIdKey = getSessionByIdKey(sessionId);

    return transaction.get(sessionByIdKey).thenAccept(currentSession -> {
      if (currentSession != null) {
        transaction.clear(sessionByIdKey);
        transaction.clear(closingKey(sessionId));
        transaction.clear(connectionKey(sessionId));
        long sessionExpirationTimestamp = Tuple.fromBytes(currentSession).getLong(0);

        // next expiration --> session ids
        transaction.clear(getSessionByTimestampKey(sessionId, sessionExpirationTimestamp));
      }
    });
  }

  @Override
  public void shutdown() {
    // N/A -- state stored in FDB, nothing to clean up here
  }

  @Override
  public void checkSession(long sessionId, Object owner) throws SessionExpiredException, SessionMovedException {
    KeeperException.Code code = fdb.run(tr -> validate(tr, sessionId, null));
    if (code == KeeperException.Code.SESSIONEXPIRED) throw new SessionExpiredException();
    if (code == KeeperException.Code.SESSIONMOVED) throw new SessionMovedException();
  }

  public KeeperException.Code validate(Transaction tr, long sessionId, byte[] epoch) {
    byte[] value = tr.get(getSessionByIdKey(sessionId)).join();
    if (value == null || tr.get(closingKey(sessionId)).join() != null
        || Tuple.fromBytes(value).getLong(0) <= clock.millis()) {
      return KeeperException.Code.SESSIONEXPIRED;
    }
    if (epoch != null && !java.util.Arrays.equals(epoch, tr.get(connectionKey(sessionId)).join())) {
      return KeeperException.Code.SESSIONMOVED;
    }
    return KeeperException.Code.OK;
  }

  public boolean beginClose(Transaction tr, long sessionId, boolean expiredOnly) {
    byte[] value = tr.get(getSessionByIdKey(sessionId)).join();
    if (value == null) return false;
    if (tr.get(closingKey(sessionId)).join() != null) return true;
    if (expiredOnly && Tuple.fromBytes(value).getLong(0) > clock.millis()) return false;
    tr.set(closingKey(sessionId), EMPTY_VALUE);
    tr.clear(connectionKey(sessionId));
    return true;
  }

  public static byte[] closingKey(long sessionId) {
    return com.ph14.fdb.zk.FdbZkKeyspace.raw().pack(Tuple.from("session-closing", sessionId));
  }

  public static byte[] connectionKey(long sessionId) {
    return com.ph14.fdb.zk.FdbZkKeyspace.raw().pack(Tuple.from("fdb-zk-watch-connection", sessionId));
  }

  @VisibleForTesting
  Tuple getSessionDataById(long sessionId) {
    return Tuple.fromBytes(fdb.read(rt -> rt.get(getSessionByIdKey(sessionId)).join()));
  }

  public List<Long> getExpiredSessionIds(long greatestExpiredSessionTimestamp) {
    List<KeyValue> keyValues = fdb.read(rt -> rt.getRange(
        new Range(
            getSessionByTimestampKey(0, 0),
            getSessionByTimestampKey(Long.MAX_VALUE, greatestExpiredSessionTimestamp)), 128)
        .asList()
        .join()
    );

    List<Long> closing = fdb.run(tr -> tr.getRange(com.ph14.fdb.zk.FdbZkKeyspace.raw()
        .range(Tuple.from("session-closing")), 128).asList().join()).stream()
        .map(kv -> com.ph14.fdb.zk.FdbZkKeyspace.raw().unpack(kv.getKey()).getLong(1))
        .collect(Collectors.toList());
    keyValues.stream()
        .map(kv -> sessionsByTimeout.unpack(kv.getKey()).getVersionstamp(1).getTransactionVersion())
        .map(Longs::fromByteArray).forEach(closing::add);
    return closing.stream().distinct().collect(Collectors.toList());
  }

  @Override
  public void setOwner(long id, Object owner) {
    // not needed, state is stored in fdb
  }

  @Override
  public void dumpSessions(PrintWriter pwriter) {
  }

  private byte[] getSessionByIdKey(long sessionId) {
    byte[] versionstampBytes = Bytes.concat(Longs.toByteArray(sessionId), Shorts.toByteArray((short) 0));
    return sessionsById.pack(Tuple.from(Versionstamp.complete(versionstampBytes)));
  }

  private byte[] getSessionByIdValue(long sessionExpirationTimestamp, long sessionTimeoutMillis) {
    return Tuple.from(sessionExpirationTimestamp, sessionTimeoutMillis).pack();
  }

  private byte[] getSessionByTimestampKey(long sessionId, long sessionExpirationTimestamp) {
    byte[] versionstampBytes = Bytes.concat(Longs.toByteArray(sessionId), Shorts.toByteArray((short) 0));
    return sessionsByTimeout.pack(Tuple.from(sessionExpirationTimestamp, Versionstamp.complete(versionstampBytes)));
  }

  private byte[] getIncompleteSessionByTimestampKey(long sessionExpirationTimestamp) {
    return sessionsByTimeout.packWithVersionstamp(Tuple.from(sessionExpirationTimestamp, Versionstamp.incomplete()));
  }

  private long calculateSessionExpirationTimestamp(long now, int timeoutMillis) {
    // We give a one interval grace period
    return ((now + timeoutMillis) / serverTickMillis + 1) * serverTickMillis;
  }

}
