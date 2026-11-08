package com.ph14.fdb.zk.layer.changefeed;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;

// Only the transaction operations used by watches. Watches and versionstamps stay pending until commit.
class WatchTestDatabase {
  private final NavigableMap<byte[], byte[]> data = new TreeMap<>(Arrays::compareUnsigned);
  private final List<Watch> watches = new ArrayList<>();
  private long version;
  boolean retryNextWrite;
  int retries;
  boolean unknownNextCommit;
  int unknownRetries;
  Runnable afterUnknownCommit = () -> {};
  final Database database = (Database) Proxy.newProxyInstance(Database.class.getClassLoader(), new Class<?>[] {Database.class},
      (proxy, method, args) -> {
        synchronized (this) {
          if (method.getName().equals("run") || method.getName().equals("runAsync")) {
            Tx tx = new Tx();
            @SuppressWarnings("unchecked")
            Object result;
            try { result = ((Function<Transaction, ?>) args[0]).apply(tx.transaction); }
            catch (RuntimeException error) {
              throw error instanceof java.util.concurrent.CompletionException ? error : new java.util.concurrent.CompletionException(error);
            }
            if (method.getName().equals("runAsync")) {
              CompletableFuture<?> future = (CompletableFuture<?>) result;
              if (!future.isDone()) {
                throw new AssertionError("Transaction callback awaited a post-commit future");
              }
              result = future.join();
            }
            if (retryNextWrite && !tx.writes.isEmpty()) {
              retryNextWrite = false;
              retries++;
              tx.transaction.cancel();
              return method.invoke(proxy, args);
            }
            tx.commit();
            if (unknownNextCommit && !tx.writes.isEmpty()) {
              unknownNextCommit = false;
              unknownRetries++;
              afterUnknownCommit.run();
              return method.invoke(proxy, args);
            }
            return method.getName().equals("runAsync") ? CompletableFuture.completedFuture(result) : result;
          }
          throw new UnsupportedOperationException(method.getName());
        }
      });

  synchronized Tx transaction() {
    return new Tx();
  }

  synchronized int armedWatches() {
    return (int) watches.stream().filter(watch -> !watch.future.isDone()).count();
  }

  class Tx {
    private final NavigableMap<byte[], byte[]> view = new TreeMap<>(Arrays::compareUnsigned);
    private final List<Runnable> writes = new ArrayList<>();
    private final List<Watch> pendingWatches = new ArrayList<>();
    private final CompletableFuture<byte[]> stamp = new CompletableFuture<>();
    private byte[] commitStamp;
    final Transaction transaction;

    Tx() {
      view.putAll(data);
      transaction = (Transaction) Proxy.newProxyInstance(Transaction.class.getClassLoader(), new Class<?>[] {Transaction.class},
          (proxy, method, args) -> {
            switch (method.getName()) {
              case "hashCode": return System.identityHashCode(proxy);
              case "equals": return proxy == args[0];
              case "snapshot": return proxy;
              case "options": return new com.apple.foundationdb.TransactionOptions((code, parameter) -> {});
              case "get": return CompletableFuture.completedFuture(view.get((byte[]) args[0]));
              case "getRange": {
                Range range = (Range) args[0];
                int limit = args.length > 1 ? (int) args[1] : Integer.MAX_VALUE;
                List<KeyValue> values = new ArrayList<>();
                view.subMap(range.begin, true, range.end, false).forEach((key, value) -> {
                  if (values.size() < limit) values.add(new KeyValue(key, value));
                });
                return Proxy.newProxyInstance(AsyncIterable.class.getClassLoader(), new Class<?>[] {AsyncIterable.class},
                    (iterable, operation, parameters) -> {
                      if (operation.getName().equals("asList")) return CompletableFuture.completedFuture(values);
                      throw new UnsupportedOperationException(operation.getName());
                    });
              }
              case "set": {
                byte[] key = (byte[]) args[0], value = (byte[]) args[1];
                view.put(key, value);
                writes.add(() -> data.put(key, value));
                return null;
              }
              case "clear": {
                if (args[0] instanceof Range || args.length == 2) {
                  Range range = args[0] instanceof Range ? (Range) args[0] : new Range((byte[]) args[0], (byte[]) args[1]);
                  view.subMap(range.begin, true, range.end, false).clear();
                  writes.add(() -> data.subMap(range.begin, true, range.end, false).clear());
                } else {
                  byte[] key = (byte[]) args[0];
                  view.remove(key);
                  writes.add(() -> data.remove(key));
                }
                return null;
              }
              case "mutate": {
                MutationType type = (MutationType) args[0];
                byte[] key = (byte[]) args[1], value = (byte[]) args[2];
                if (type == MutationType.SET_VERSIONSTAMPED_KEY) writes.add(() -> data.put(stamped(key), value));
                else if (type == MutationType.SET_VERSIONSTAMPED_VALUE) writes.add(() -> data.put(key, stamped(value)));
                else throw new UnsupportedOperationException(type.toString());
                return null;
              }
              case "watch": {
                Watch watch = new Watch((byte[]) args[0], view.get((byte[]) args[0]));
                pendingWatches.add(watch);
                return watch.future;
              }
              case "getVersionstamp": return stamp;
              case "commit": commit(); return CompletableFuture.completedFuture(null);
              case "cancel":
              case "close":
                pendingWatches.forEach(watch -> watch.future.cancel(false));
                stamp.cancel(false);
                return null;
              default: throw new UnsupportedOperationException(method.getName());
            }
          });
    }

    void commit() {
      synchronized (WatchTestDatabase.this) {
        commitStamp = ByteBuffer.allocate(10).putLong(++version).putShort((short) 0).array();
        writes.forEach(Runnable::run);
        List<Watch> triggered = new ArrayList<>();
        for (Watch watch : watches) {
          if (!Arrays.equals(watch.value, data.get(watch.key))) triggered.add(watch);
        }
        watches.removeAll(triggered);
        watches.removeIf(watch -> watch.future.isDone());
        watches.addAll(pendingWatches);
        stamp.complete(commitStamp);
        triggered.forEach(watch -> watch.future.complete(null));
      }
    }

    private byte[] stamped(byte[] packed) {
      int offset = ByteBuffer.wrap(packed, packed.length - 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      byte[] result = Arrays.copyOf(packed, packed.length - 4);
      System.arraycopy(commitStamp, 0, result, offset, 10);
      return result;
    }
  }

  private static class Watch {
    final byte[] key;
    final byte[] value;
    final CompletableFuture<Void> future = new CompletableFuture<>();

    Watch(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }
  }
}
