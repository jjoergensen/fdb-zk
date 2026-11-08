package com.ph14.fdb.zk.ops;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiOperationRecord;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult.CheckResult;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.OpResult.DeleteResult;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbAcl;

public class FdbMultiOp {

  private final Database fdb;
  private final FdbAcl acls;
  private final FdbCreateOp fdbCreateOp;
  private final FdbDeleteOp fdbDeleteOp;
  private final FdbSetDataOp fdbSetDataOp;
  private final FdbCheckVersionOp fdbCheckVersionOp;

  @Inject
  public FdbMultiOp(Database fdb,
                    FdbCreateOp fdbCreateOp,
                    FdbDeleteOp fdbDeleteOp,
                    FdbSetDataOp fdbSetDataOp,
                    FdbCheckVersionOp fdbCheckVersionOp, FdbAcl acls) {
    this.fdb = fdb;
    this.acls = acls;
    this.fdbCreateOp = fdbCreateOp;
    this.fdbDeleteOp = fdbDeleteOp;
    this.fdbSetDataOp = fdbSetDataOp;
    this.fdbCheckVersionOp = fdbCheckVersionOp;
  }

  public MultiResponse execute(Request request, MultiOperationRecord operations) {
    try {
      return fdb.run(tr -> prepare(request, tr, operations));
    } catch (RuntimeException error) {
      RuntimeException cause = FdbOp.unwrap(error);
      if (!(cause instanceof AbortTransactionException)) throw cause;
      return failure(operations, (AbortTransactionException) cause);
    }
  }

  public MultiResponse prepare(Request zkRequest, Transaction transaction,
                               MultiOperationRecord multiTransactionRecord) {
    List<OpResult> attemptResults = new ArrayList<>();
    for (Op op : multiTransactionRecord) {
      Result<OpResult, KeeperException> prepared;

      try {
        acls.authorize(zkRequest, transaction, op.toRequestRecord());
      } catch (FdbAcl.Failure denied) {
        throw new AbortTransactionException(attemptResults.size(), denied.error.code());
      }
      switch (op.getType()) {
        case OpCode.create:
          prepared = fdbCreateOp.execute(zkRequest, transaction, (CreateRequest) op.toRequestRecord()).join()
              .mapOk(response -> new CreateResult(response.getPath()));
          break;
        case OpCode.create2:
          prepared = fdbCreateOp.prepare(zkRequest, transaction, (CreateRequest) op.toRequestRecord())
              .mapOk(created -> new CreateResult(created.getPath(), created.getStat()));
          break;
        case OpCode.delete:
          prepared = fdbDeleteOp.execute(zkRequest, transaction, (DeleteRequest) op.toRequestRecord()).join()
              .mapOk(response -> new DeleteResult());
          break;
        case OpCode.setData:
          prepared = fdbSetDataOp.prepare(zkRequest, transaction, (SetDataRequest) op.toRequestRecord())
              .mapOk(updated -> new SetDataResult(updated.getStat()));
          break;
        case OpCode.check:
          prepared = fdbCheckVersionOp.execute(zkRequest, transaction, (CheckVersionRequest) op.toRequestRecord()).join()
              .mapOk(response -> new CheckResult());
          break;
        default:
          prepared = Result.err(new KeeperException.UnimplementedException());
      }

      if (prepared.isErr()) {
        throw new AbortTransactionException(attemptResults.size(), prepared.unwrapErrOrElseThrow().code());
      }
      attemptResults.add(prepared.unwrapOrElseThrow());
    }

    MultiResponse response = new MultiResponse();
    for (OpResult result : attemptResults) response.add(result);
    return response;
  }

  public MultiResponse failure(MultiOperationRecord operations, AbortTransactionException aborted) {
    MultiResponse response = new MultiResponse();
    int index = 0;
    for (Op op : operations) {
      KeeperException.Code code = index < aborted.index ? KeeperException.Code.OK
          : index == aborted.index ? aborted.code : KeeperException.Code.RUNTIMEINCONSISTENCY;
      response.add(new ErrorResult(code.intValue()));
      index++;
    }
    return response;
  }


  public static class AbortTransactionException extends RuntimeException {
    private final int index;
    private final KeeperException.Code code;

    public AbortTransactionException(int index, KeeperException.Code code) {
      this.index = index;
      this.code = code;
    }
  }

}
