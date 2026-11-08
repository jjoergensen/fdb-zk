package com.ph14.fdb.zk;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.zookeeper.data.ACL;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.primitives.Longs;
import com.ph14.fdb.zk.layer.StatKey;

public final class FdbTransactionState {
  private static final Map<Transaction, FdbTransactionState> ATTEMPTS = new WeakHashMap<>();
  private long zxid;
  private boolean allocated;
  private long eventOrdinal;
  public final Map<String, List<ACL>> eventAcls = new HashMap<>();
  public final Map<ByteBuffer, Map<StatKey, Long>> stats = new HashMap<>();

  private FdbTransactionState(Transaction tr) {
    byte[] value = tr.snapshot().get(zxidKey()).join();
    zxid = value == null ? 0 : Longs.fromByteArray(value);
  }

  public static FdbTransactionState of(Transaction tr) {
    synchronized (ATTEMPTS) {
      FdbTransactionState existing = ATTEMPTS.get(tr);
      if (existing != null) return existing;
    }
    FdbTransactionState created = new FdbTransactionState(tr);
    synchronized (ATTEMPTS) {
      return ATTEMPTS.computeIfAbsent(tr, ignored -> created);
    }
  }

  public static byte[] zxidKey() {
    return FdbZkKeyspace.raw().pack(Tuple.from("zxid"));
  }

  public static long current(Transaction tr) {
    byte[] value = tr.get(zxidKey()).join();
    return value == null ? 0 : Longs.fromByteArray(value);
  }

  public synchronized long nextEventOrdinal() {
    return ++eventOrdinal;
  }

  public long position() {
    return zxid;
  }

  public long mutate(Transaction tr) {
    if (!allocated) {
      zxid = Math.addExact(current(tr), 1);
      tr.set(zxidKey(), Longs.toByteArray(zxid));
      allocated = true;
    }
    return zxid;
  }
}
