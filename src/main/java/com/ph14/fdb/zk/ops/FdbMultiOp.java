package com.ph14.fdb.zk.ops;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.MultiOperationRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.CheckResult;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.OpResult.DeleteResult;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Database;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;

/**
 * Not functional yet... many of these ops need atomic mutations
 * which aren't read-your-writes friendly.
 *
 * Tracking: https://github.com/pH14/fdb-zk/issues/15
 */
public class FdbMultiOp {

  private final Database fdb;
  private final FdbCreateOp fdbCreateOp;
  private final FdbDeleteOp fdbDeleteOp;
  private final FdbSetDataOp fdbSetDataOp;
  private final FdbCheckVersionOp fdbCheckVersionOp;

  @Inject
  public FdbMultiOp(Database fdb,
                    FdbCreateOp fdbCreateOp,
                    FdbDeleteOp fdbDeleteOp,
                    FdbSetDataOp fdbSetDataOp,
                    FdbCheckVersionOp fdbCheckVersionOp) {
    this.fdb = fdb;
    this.fdbCreateOp = fdbCreateOp;
    this.fdbDeleteOp = fdbDeleteOp;
    this.fdbSetDataOp = fdbSetDataOp;
    this.fdbCheckVersionOp = fdbCheckVersionOp;
  }

  public MultiResponse execute(Request zkRequest, MultiOperationRecord multiTransactionRecord) {
    final List<CompletableFuture<OpResult>> results;
    try {
      results = fdb.run(transaction -> {
        List<CompletableFuture<OpResult>> attemptResults = new ArrayList<>();
        for (Op op : multiTransactionRecord) {
          Result<CompletableFuture<OpResult>, KeeperException> prepared;

          switch (op.getType()) {
            case OpCode.create:
              prepared = fdbCreateOp.execute(zkRequest, transaction, (CreateRequest) op.toRequestRecord()).join()
                  .mapOk(response -> CompletableFuture.completedFuture(new CreateResult(response.getPath())));
              break;
            case OpCode.create2:
              prepared = fdbCreateOp.prepare(zkRequest, transaction, (CreateRequest) op.toRequestRecord())
                  .mapOk(response -> response.thenApply(created -> new CreateResult(created.getPath(), created.getStat())));
              break;
            case OpCode.delete:
              prepared = fdbDeleteOp.execute(zkRequest, transaction, (DeleteRequest) op.toRequestRecord()).join()
                  .mapOk(response -> CompletableFuture.completedFuture(new DeleteResult()));
              break;
            case OpCode.setData:
              prepared = fdbSetDataOp.prepare(zkRequest, transaction, (SetDataRequest) op.toRequestRecord())
                  .mapOk(response -> response.thenApply(updated -> new SetDataResult(updated.getStat())));
              break;
            case OpCode.check:
              prepared = fdbCheckVersionOp.execute(zkRequest, transaction, (CheckVersionRequest) op.toRequestRecord()).join()
                  .mapOk(response -> CompletableFuture.completedFuture(new CheckResult()));
              break;
            default:
              prepared = Result.err(new KeeperException.UnimplementedException());
          }

          if (prepared.isErr()) {
            throw new AbortTransactionException(attemptResults.size(), prepared.unwrapErrOrElseThrow().code());
          }
          attemptResults.add(prepared.unwrapOrElseThrow());
        }

        return attemptResults;
      });
    } catch (RuntimeException e) {
      Throwable cause = e;
      while (cause instanceof CompletionException) {
        cause = cause.getCause();
      }
      if (!(cause instanceof AbortTransactionException)) {
        throw e;
      }
      AbortTransactionException aborted = (AbortTransactionException) cause;
      MultiResponse response = new MultiResponse();
      int index = 0;
      for (Op op : multiTransactionRecord) {
        KeeperException.Code code = index < aborted.index ? KeeperException.Code.OK
            : index == aborted.index ? aborted.code : KeeperException.Code.RUNTIMEINCONSISTENCY;
        response.add(new ErrorResult(code.intValue()));
        index++;
      }
      return response;
    }

    MultiResponse multiResponse = new MultiResponse();
    for (CompletableFuture<OpResult> result : results) {
      multiResponse.add(result.join());
    }
    return multiResponse;
  }


  private static class AbortTransactionException extends RuntimeException {
    private final int index;
    private final KeeperException.Code code;

    public AbortTransactionException(int index, KeeperException.Code code) {
      this.index = index;
      this.code = code;
    }
  }

}
