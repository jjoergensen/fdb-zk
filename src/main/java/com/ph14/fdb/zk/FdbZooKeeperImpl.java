package com.ph14.fdb.zk;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.MultiOperationRecord;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.SetDataRequest;
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
import com.ph14.fdb.zk.ops.FdbOp;
import com.ph14.fdb.zk.ops.FdbSetDataOp;
import com.ph14.fdb.zk.ops.FdbSetWatchesOp;
import com.ph14.fdb.zk.session.FdbSessionManager;

public class FdbZooKeeperImpl {

  private static final Set<Integer> OPS_WITHOUT_SESSION_CHECK = ImmutableSet.<Integer>builder()
      .add(OpCode.createSession)
      .add(OpCode.closeSession)
      .build();

  private static final Set<Integer> FDB_SUPPORTED_OPCODES = ImmutableSet.<Integer>builder()
      .addAll(
          Arrays.asList(
              OpCode.create,
              OpCode.create2,
              OpCode.delete,
              OpCode.setData,
              OpCode.multi,
              OpCode.exists,
              OpCode.getData,
              OpCode.getACL,
              OpCode.getChildren,
              OpCode.getChildren2, // includes stat of node
              OpCode.setWatches,
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
  private final FdbCreateOp fdbCreateOp;
  private final FdbCheckVersionOp fdbCheckVersionOp;
  private final FdbExistsOp fdbExistsOp;
  private final FdbGetDataOp fdbGetDataOp;
  private final FdbSetDataOp fdbSetDataOp;
  private final FdbGetChildrenOp fdbGetChildrenOp;
  private final FdbGetChildrenWithStatOp fdbGetChildrenWithStatOp;
  private final FdbDeleteOp fdbDeleteOp;
  private final FdbSetWatchesOp fdbSetWatchesOp;
  private final FdbMultiOp fdbMultiOp;
  private final com.ph14.fdb.zk.session.FdbSessionDataPurger fdbSessionDataPurger;

  private final FdbSessionManager fdbSessionManager;

  @Inject
  public FdbZooKeeperImpl(Database fdb,
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
                          FdbMultiOp fdbMultiOp,
                          com.ph14.fdb.zk.session.FdbSessionDataPurger fdbSessionDataPurger) {
    this.fdbSessionDataPurger = fdbSessionDataPurger;
    this.fdb = fdb;
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
    this.fdbMultiOp = fdbMultiOp;
  }

  public boolean handlesRequest(Request request) {
    return FDB_SUPPORTED_OPCODES.contains(request.type);
  }

  public Result<?, KeeperException> handle(Request request) throws IOException {
    if (!handlesRequest(request)) {
      return Result.err(new KeeperException.UnimplementedException());
    }

    if (!OPS_WITHOUT_SESSION_CHECK.contains(request.type)) {
      try {
        fdbSessionManager.checkSession(request.sessionId, null);
      } catch (SessionExpiredException | SessionMovedException e) {
        return Result.err(e);
      }
    }

    switch (request.type) {
      case OpCode.create:
        CreateRequest createRequest = request.readRequestRecord(CreateRequest::new);
        return fdb.runAsync(tr -> fdbCreateOp.execute(request, tr, createRequest)).join();

      case OpCode.create2:
        CreateRequest create2Request = request.readRequestRecord(CreateRequest::new);
        return fdb.runAsync(tr -> CompletableFuture.completedFuture(fdbCreateOp.prepare(request, tr, create2Request)))
            .thenCompose(FdbOp::afterCommit)
            .join();

      case OpCode.exists:
        ExistsRequest existsRequest = request.readRequestRecord(ExistsRequest::new);
        return fdb.runAsync(tr -> fdbExistsOp.execute(request, tr, existsRequest)).join();

      case OpCode.delete:
        DeleteRequest deleteRequest = request.readRequestRecord(DeleteRequest::new);
        return fdb.runAsync(tr -> fdbDeleteOp.execute(request, tr, deleteRequest))
            .thenApply(r -> r.mapOk(success -> deleteRequest))
            .join();

      case OpCode.getData:
        GetDataRequest getDataRequest = request.readRequestRecord(GetDataRequest::new);
        return fdb.runAsync(tr -> fdbGetDataOp.execute(request, tr, getDataRequest)).join();

      case OpCode.getACL: {
        // Listed as supported upstream but never implemented (fell through to BadArguments); Solr calls it at startup.
        org.apache.zookeeper.proto.GetACLRequest getAclRequest = request.readRequestRecord(org.apache.zookeeper.proto.GetACLRequest::new);
        return fdb.run(tr -> {
          try {
            var dir = com.ph14.fdb.zk.FdbZkKeyspace.dir()
                .open(tr, com.ph14.fdb.zk.layer.FdbPath.toFdbPath(getAclRequest.getPath())).join();
            var node = new com.ph14.fdb.zk.layer.FdbNodeReader().getNode(dir, tr).join();
            return Result.ok(new org.apache.zookeeper.proto.GetACLResponse(node.getAcls(), node.getStat()));
          } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof com.apple.foundationdb.directory.NoSuchDirectoryException) {
              return Result.err(new KeeperException.NoNodeException(getAclRequest.getPath()));
            }
            throw e;
          }
        });
      }

      case OpCode.getChildren:
        GetChildrenRequest getChildrenRequest = request.readRequestRecord(GetChildrenRequest::new);
        return fdb.runAsync(tr -> fdbGetChildrenOp.execute(request, tr, getChildrenRequest)).join();

      case OpCode.getChildren2:
        GetChildren2Request getChildren2Request = request.readRequestRecord(GetChildren2Request::new);
        return fdb.runAsync(tr -> fdbGetChildrenWithStatOp.execute(request, tr, getChildren2Request)).join();

      case OpCode.setData:
        SetDataRequest setDataRequest = request.readRequestRecord(SetDataRequest::new);
        return fdb.runAsync(tr -> CompletableFuture.completedFuture(fdbSetDataOp.prepare(request, tr, setDataRequest)))
            .thenCompose(FdbOp::afterCommit)
            .join();

      case OpCode.check:
        CheckVersionRequest checkVersionRequest = request.readRequestRecord(CheckVersionRequest::new);
        return fdb.runAsync(tr -> fdbCheckVersionOp.execute(request, tr, checkVersionRequest)).join();

      case OpCode.setWatches:
        SetWatches setWatches = request.readRequestRecord(SetWatches::new);
        return fdb.runAsync(tr -> fdbSetWatchesOp.execute(request, tr, setWatches))
            .thenApply(result -> result.mapOk(events -> {
              events.forEach(request.cnxn::process);
              return Boolean.TRUE;
            }))
            .join();

      case OpCode.whoAmI:
        return Result.ok(new WhoAmIResponse(AuthUtil.getClientInfos(request.authInfo)));

      case OpCode.createSession:
        int to = java.nio.ByteBuffer.wrap(request.readRequestBytes()).getInt();
        request.setTxn(new CreateSessionTxn(to));
        fdbSessionManager.addSession(request.sessionId, to);
        return Result.ok(true);

      case OpCode.closeSession:
        // ZooKeeper semantics: closing a session deletes its ephemeral nodes (firing watches) at once.
        // Upstream only removed the session keys, which also removed it from the expiry index, so the
        // ephemerals of cleanly closed sessions were never deleted (stale Solr live_nodes).
        fdbSessionDataPurger.removeAllSessionData(request.sessionId).join();
        return Result.ok(true);

      case OpCode.multi:
        MultiOperationRecord multiTransactionRecord = request.readRequestRecord(MultiOperationRecord::new);
        return Result.ok(fdbMultiOp.execute(request, multiTransactionRecord));

      case OpCode.sync: // no-op, fdb won't return stale reads
        SyncRequest syncRequest = request.readRequestRecord(SyncRequest::new);
        return Result.ok(new SyncResponse(syncRequest.getPath()));

      case OpCode.ping:
        return Result.ok(true);
    }

    return Result.err(new KeeperException.UnimplementedException());
  }

}
