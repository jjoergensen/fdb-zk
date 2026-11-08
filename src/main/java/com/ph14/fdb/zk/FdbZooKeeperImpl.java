package com.ph14.fdb.zk;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiOperationRecord;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.AddWatchRequest;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CheckWatchesRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.RemoveWatchesRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetWatches2;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.proto.WhoAmIResponse;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.util.AuthUtil;
import org.apache.zookeeper.txn.CreateSessionTxn;

import com.apple.foundationdb.Database;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.ops.FdbCheckVersionOp;
import com.ph14.fdb.zk.ops.FdbCreateOp;
import com.ph14.fdb.zk.ops.FdbDeleteOp;
import com.ph14.fdb.zk.ops.FdbExistsOp;
import com.ph14.fdb.zk.ops.FdbGetChildrenOp;
import com.ph14.fdb.zk.ops.FdbGetChildrenWithStatOp;
import com.ph14.fdb.zk.ops.FdbGetDataOp;
import com.ph14.fdb.zk.ops.FdbMultiOp;
import com.ph14.fdb.zk.ops.FdbSetDataOp;
import com.ph14.fdb.zk.ops.FdbSetWatchesOp;
import com.ph14.fdb.zk.ops.FdbWatchOp;
import com.ph14.fdb.zk.session.FdbSessionManager;

public class FdbZooKeeperImpl {

  private static final Set<Integer> FDB_SUPPORTED_OPCODES = ImmutableSet.<Integer>builder()
      .addAll(
          Arrays.asList(
              OpCode.create,
              OpCode.create2,
              OpCode.delete,
              OpCode.setData,
              OpCode.setACL,
              OpCode.multi,
              OpCode.exists,
              OpCode.getData,
              OpCode.getACL,
              OpCode.getChildren,
              OpCode.getChildren2, // includes stat of node
              OpCode.setWatches,
              OpCode.setWatches2,
              OpCode.addWatch,
              OpCode.checkWatches,
              OpCode.removeWatches,
              OpCode.check,
              OpCode.whoAmI,

              OpCode.sync,
              OpCode.ping,

              OpCode.createSession,
              OpCode.closeSession
          )
      )
      .build();

  private final Database fdb;
  private final FdbRequestTransactions transactions;
  private final FdbAcl acls;
  private final FdbCreateOp fdbCreateOp;
  private final FdbCheckVersionOp fdbCheckVersionOp;
  private final FdbExistsOp fdbExistsOp;
  private final FdbGetDataOp fdbGetDataOp;
  private final FdbSetDataOp fdbSetDataOp;
  private final FdbGetChildrenOp fdbGetChildrenOp;
  private final FdbGetChildrenWithStatOp fdbGetChildrenWithStatOp;
  private final FdbDeleteOp fdbDeleteOp;
  private final FdbSetWatchesOp fdbSetWatchesOp;
  private final FdbWatchOp fdbWatchOp;
  private final FdbMultiOp fdbMultiOp;
  private final com.ph14.fdb.zk.session.FdbSessionDataPurger fdbSessionDataPurger;

  private final FdbSessionManager fdbSessionManager;

  @Inject
  public FdbZooKeeperImpl(Database fdb,
                          FdbRequestTransactions transactions,
                          FdbAcl acls,
                          FdbSessionManager fdbSessionManager,
                          FdbCreateOp fdbCreateOp,
                          FdbCheckVersionOp fdbCheckVersionOp,
                          FdbExistsOp fdbExistsOp,
                          FdbGetDataOp fdbGetDataOp,
                          FdbSetDataOp fdbSetDataOp,
                          FdbGetChildrenOp fdbGetChildrenOp,
                          FdbGetChildrenWithStatOp fdbGetChildrenWithStatOp,
                          FdbDeleteOp fdbDeleteOp,
                          FdbSetWatchesOp fdbSetWatchesOp,
                          FdbWatchOp fdbWatchOp,
                          FdbMultiOp fdbMultiOp,
                          com.ph14.fdb.zk.session.FdbSessionDataPurger fdbSessionDataPurger) {
    this.fdbSessionDataPurger = fdbSessionDataPurger;
    this.fdb = fdb;
    this.transactions = transactions;
    this.acls = acls;
    this.fdbSessionManager = fdbSessionManager;
    this.fdbCreateOp = fdbCreateOp;
    this.fdbCheckVersionOp = fdbCheckVersionOp;
    this.fdbExistsOp = fdbExistsOp;
    this.fdbGetDataOp = fdbGetDataOp;
    this.fdbSetDataOp = fdbSetDataOp;
    this.fdbGetChildrenOp = fdbGetChildrenOp;
    this.fdbGetChildrenWithStatOp = fdbGetChildrenWithStatOp;
    this.fdbDeleteOp = fdbDeleteOp;
    this.fdbSetWatchesOp = fdbSetWatchesOp;
    this.fdbWatchOp = fdbWatchOp;
    this.fdbMultiOp = fdbMultiOp;
  }

  public boolean handlesRequest(Request request) {
    return FDB_SUPPORTED_OPCODES.contains(request.type);
  }

  public long currentZxid() {
    return fdb.run(FdbTransactionState::current);
  }

  public Result<?, KeeperException> handle(Request request) throws IOException {
    if (!handlesRequest(request)) {
      return Result.err(new KeeperException.UnimplementedException());
    }

    switch (request.type) {
      case OpCode.create:
        CreateRequest createRequest = request.readRequestRecord(CreateRequest::new);
        return transactions.run(request, createRequest, tr -> fdbCreateOp.execute(request, tr, createRequest).join());

      case OpCode.create2:
        CreateRequest create2Request = request.readRequestRecord(CreateRequest::new);
        return transactions.run(request, create2Request, tr -> fdbCreateOp.prepare(request, tr, create2Request));

      case OpCode.exists:
        ExistsRequest existsRequest = request.readRequestRecord(ExistsRequest::new);
        return transactions.run(request, existsRequest, tr -> fdbExistsOp.execute(request, tr, existsRequest).join());

      case OpCode.delete:
        DeleteRequest deleteRequest = request.readRequestRecord(DeleteRequest::new);
        return transactions.run(request, deleteRequest, tr -> fdbDeleteOp.execute(request, tr, deleteRequest).join().mapOk(success -> Boolean.TRUE));

      case OpCode.getData:
        GetDataRequest getDataRequest = request.readRequestRecord(GetDataRequest::new);
        return transactions.run(request, getDataRequest, tr -> fdbGetDataOp.execute(request, tr, getDataRequest).join());

      case OpCode.getACL: {
        org.apache.zookeeper.proto.GetACLRequest command = request.readRequestRecord(org.apache.zookeeper.proto.GetACLRequest::new);
        return transactions.run(request, command, tr -> Result.ok(acls.getAcl(request, tr, command)));
      }

      case OpCode.setACL: {
        org.apache.zookeeper.proto.SetACLRequest command = request.readRequestRecord(org.apache.zookeeper.proto.SetACLRequest::new);
        return transactions.run(request, command, tr -> Result.ok(acls.setAcl(request, tr, command)));
      }

      case OpCode.getChildren:
        GetChildrenRequest getChildrenRequest = request.readRequestRecord(GetChildrenRequest::new);
        return transactions.run(request, getChildrenRequest, tr -> fdbGetChildrenOp.execute(request, tr, getChildrenRequest).join());

      case OpCode.getChildren2:
        GetChildren2Request getChildren2Request = request.readRequestRecord(GetChildren2Request::new);
        return transactions.run(request, getChildren2Request, tr -> fdbGetChildrenWithStatOp.execute(request, tr, getChildren2Request).join());

      case OpCode.setData:
        SetDataRequest setDataRequest = request.readRequestRecord(SetDataRequest::new);
        return transactions.run(request, setDataRequest, tr -> fdbSetDataOp.prepare(request, tr, setDataRequest));

      case OpCode.check:
        CheckVersionRequest checkVersionRequest = request.readRequestRecord(CheckVersionRequest::new);
        return transactions.run(request, checkVersionRequest, tr -> fdbCheckVersionOp.execute(request, tr, checkVersionRequest).join());

      case OpCode.setWatches:
        SetWatches setWatches = request.readRequestRecord(SetWatches::new);
        return transactions.run(request, tr -> fdbSetWatchesOp.execute(request, tr, setWatches).join());

      case OpCode.setWatches2:
        SetWatches2 setWatches2 = request.readRequestRecord(SetWatches2::new);
        return transactions.run(request, tr -> fdbSetWatchesOp.execute(request, tr, setWatches2).join());

      case OpCode.addWatch:
        AddWatchRequest addWatch = request.readRequestRecord(AddWatchRequest::new);
        return transactions.run(request, tr -> fdbWatchOp.execute(request, tr, addWatch).join());

      case OpCode.checkWatches:
        CheckWatchesRequest checkWatches = request.readRequestRecord(CheckWatchesRequest::new);
        return transactions.run(request, tr -> fdbWatchOp.execute(request, tr, checkWatches).join());

      case OpCode.removeWatches:
        RemoveWatchesRequest removeWatches = request.readRequestRecord(RemoveWatchesRequest::new);
        return transactions.run(request, tr -> fdbWatchOp.execute(request, tr, removeWatches).join());

      case OpCode.whoAmI:
        return transactions.run(request, tr -> Result.ok(new WhoAmIResponse(AuthUtil.getClientInfos(request.authInfo))));

      case OpCode.createSession:
        int to = java.nio.ByteBuffer.wrap(request.readRequestBytes()).getInt();
        request.setTxn(new CreateSessionTxn(to));
        return fdbSessionManager.commitSession(request.sessionId, to) ? Result.ok(true)
            : Result.err(new KeeperException.SessionExpiredException());

      case OpCode.closeSession:
        Result<Boolean, KeeperException> closed = fdbSessionDataPurger.close(request, transactions).join();
        request.zxid = currentZxid();
        return closed;

      case OpCode.multi:
        MultiOperationRecord multiTransactionRecord = request.readRequestRecord(MultiOperationRecord::new);
        try {
          return transactions.run(request, tr -> Result.ok(fdbMultiOp.prepare(request, tr, multiTransactionRecord)));
        } catch (FdbMultiOp.AbortTransactionException aborted) {
          request.zxid = currentZxid();
          return Result.ok(fdbMultiOp.failure(multiTransactionRecord, aborted));
        }

      case OpCode.sync: // no-op, fdb won't return stale reads
        SyncRequest syncRequest = request.readRequestRecord(SyncRequest::new);
        return transactions.run(request, tr -> Result.ok(new SyncResponse(syncRequest.getPath())));

      case OpCode.ping:
        return transactions.run(request, tr -> Result.ok(true));
    }

    return Result.err(new KeeperException.UnimplementedException());
  }

}
