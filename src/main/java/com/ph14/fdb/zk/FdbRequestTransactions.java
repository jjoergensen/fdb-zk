package com.ph14.fdb.zk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.Create2Response;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.changefeed.SessionWatchFeed;
import com.ph14.fdb.zk.layer.changefeed.WatchEventChangefeed;
import com.ph14.fdb.zk.ops.FdbOp;
import com.ph14.fdb.zk.session.FdbSessionManager;

public class FdbRequestTransactions {
  private final Database database;
  private final FdbAcl acls;
  private final FdbSessionManager sessions;
  private final SessionWatchFeed feed;
  private final WatchEventChangefeed changes;

  @Inject
  public FdbRequestTransactions(Database database, FdbSessionManager sessions,
                                SessionWatchFeed feed, WatchEventChangefeed changes, FdbAcl acls) {
    this.database = database;
    this.acls = acls;
    this.sessions = sessions;
    this.feed = feed;
    this.changes = changes;
  }

  public Result<?, KeeperException> run(Request request, Record command,
                                       Function<Transaction, Result<?, KeeperException>> action) {
    return run(request, tr -> { acls.authorize(request, tr, command); return action.apply(tr); });
  }

  public Result<?, KeeperException> run(Request request, Function<Transaction, Result<?, KeeperException>> action) {
    byte[] epoch = feed.epoch(request);
    if (epoch == null) return Result.err(new KeeperException.SessionMovedException());
    boolean receipted = isReplayable(request.type);
    boolean restoring = request.type == OpCode.setWatches || request.type == OpCode.setWatches2;
    while (true) {
      try {
        Outcome outcome = database.run(tr -> {
          KeeperException.Code code = sessions.validate(tr, request.sessionId, epoch);
          if (code != KeeperException.Code.OK) return new Outcome(Result.err(KeeperException.create(code)), FdbTransactionState.current(tr));
          byte[] receiptKey = receiptKey(request.sessionId, request.cxid);
          if (receipted) {
            List<KeyValue> receipt = tr.getRange(Range.startsWith(receiptKey)).asList().join();
            if (!receipt.isEmpty()) return decode(request.type, receipt);
          }
          FdbTransactionState state = FdbTransactionState.of(tr);
          if (!restoring && !changes.expand(tr, request.sessionId, state.position())) throw new CatchUp();
          if (!restoring) changes.finishRestoration(tr, request.sessionId);
          Result<?, KeeperException> result = action.apply(tr);
          if (result.isErr() && !(request.type == OpCode.exists
              && result.unwrapErrOrElseThrow().code() == KeeperException.Code.NONODE)) {
            throw new Failed(result.unwrapErrOrElseThrow());
          }
          Outcome prepared = new Outcome(result, state.position());
          if (receipted) {
            // Requests run serially; the successor can retire this receipt once its FDB retry has resolved.
            byte[] lastKey = FdbZkKeyspace.raw().pack(Tuple.from("request-last", request.sessionId));
            byte[] previous = tr.get(lastKey).join();
            if (previous != null && !Arrays.equals(previous, receiptKey)) tr.clear(Range.startsWith(previous));
            tr.set(lastKey, receiptKey);
            byte[] value = encode(prepared);
            for (int offset = 0, part = 0; offset < value.length; offset += 80000, part++) {
              tr.set(ByteArrayUtil.join(receiptKey, Tuple.from(part).pack()),
                  Arrays.copyOfRange(value, offset, Math.min(value.length, offset + 80000)));
            }
          }
          return prepared;
        });
        request.zxid = outcome.zxid;
        return outcome.result;
      } catch (RuntimeException error) {
        RuntimeException cause = FdbOp.unwrap(error);
        if (cause instanceof CatchUp) {
          feed.beforeReply(request.sessionId, database.run(FdbTransactionState::current));
          continue;
        }
        KeeperException failure;
        if (cause instanceof FdbAcl.Failure) failure = ((FdbAcl.Failure) cause).error;
        else if (cause instanceof Failed) failure = ((Failed) cause).error;
        else throw cause;
        request.zxid = database.run(FdbTransactionState::current);
        return Result.err(failure);
      }
    }
  }

  private static boolean isReplayable(int type) {
    return type == OpCode.create || type == OpCode.create2 || type == OpCode.setData
        || type == OpCode.delete || type == OpCode.multi || type == OpCode.setACL
        || type == OpCode.exists || type == OpCode.getData || type == OpCode.getChildren
        || type == OpCode.getChildren2 || type == OpCode.addWatch || type == OpCode.removeWatches
        || type == OpCode.checkWatches || type == OpCode.check;
  }

  private static byte[] receiptKey(long session, int cxid) {
    return FdbZkKeyspace.raw().pack(Tuple.from("request-receipt", session, cxid));
  }

  public static void clearReceipts(Transaction tr, long session) {
    tr.clear(FdbZkKeyspace.raw().range(Tuple.from("request-receipt", session)));
    tr.clear(FdbZkKeyspace.raw().pack(Tuple.from("request-last", session)));
  }

  private static byte[] encode(Outcome outcome) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      BinaryOutputArchive archive = BinaryOutputArchive.getArchive(bytes);
      archive.writeLong(outcome.zxid, "zxid");
      archive.writeInt(outcome.result.isErr() ? outcome.result.unwrapErrOrElseThrow().code().intValue() : 0, "error");
      if (outcome.result.isOk()) {
        Object value = outcome.result.unwrapOrElseThrow();
        if (value instanceof Record) ((Record) value).serialize(archive, "result");
      }
      return bytes.toByteArray();
    } catch (IOException e) { throw new java.io.UncheckedIOException(e); }
  }

  private static Outcome decode(int type, List<KeyValue> parts) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      for (KeyValue part : parts) bytes.write(part.getValue());
      BinaryInputArchive archive = BinaryInputArchive.getArchive(new ByteArrayInputStream(bytes.toByteArray()));
      long zxid = archive.readLong("zxid");
      int error = archive.readInt("error");
      if (error != 0) return new Outcome(Result.err(KeeperException.create(KeeperException.Code.get(error))), zxid);
      Record response;
      switch (type) {
        case OpCode.create: response = new CreateResponse(); break;
        case OpCode.create2: response = new Create2Response(); break;
        case OpCode.setData: response = new SetDataResponse(); break;
        case OpCode.setACL: response = new SetACLResponse(); break;
        case OpCode.multi: response = new MultiResponse(); break;
        case OpCode.getData: response = new GetDataResponse(); break;
        case OpCode.exists: response = new ExistsResponse(); break;
        case OpCode.getChildren: response = new GetChildrenResponse(); break;
        case OpCode.getChildren2: response = new GetChildren2Response(); break;
        default: response = null;
      }
      if (response != null) response.deserialize(archive, "result");
      return new Outcome(Result.ok(response == null ? Boolean.TRUE : response), zxid);
    } catch (IOException e) { throw new java.io.UncheckedIOException(e); }
  }

  private static class Outcome {
    final Result<?, KeeperException> result;
    final long zxid;
    Outcome(Result<?, KeeperException> result, long zxid) { this.result = result; this.zxid = zxid; }
  }
  private static class CatchUp extends RuntimeException {}
  private static class Failed extends RuntimeException {
    final KeeperException error;
    Failed(KeeperException error) { this.error = error; }
  }
}
