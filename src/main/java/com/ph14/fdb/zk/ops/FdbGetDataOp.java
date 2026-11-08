package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbNode;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.FdbWatchManager;

public class FdbGetDataOp implements FdbOp<GetDataRequest, GetDataResponse> {

  private final FdbNodeReader fdbNodeReader;
  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbGetDataOp(FdbNodeReader fdbNodeReader,
                      FdbWatchManager fdbWatchManager) {
    this.fdbNodeReader = fdbNodeReader;
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public CompletableFuture<Result<GetDataResponse, KeeperException>> execute(Request zkRequest, Transaction transaction, GetDataRequest request) {
    List<String> path = FdbPath.toFdbPath(request.getPath());

    fdbWatchManager.checkForWatches(zkRequest.sessionId, zkRequest.cnxn);

    final DirectorySubspace subspace;
    try {
      subspace = com.ph14.fdb.zk.FdbZkKeyspace.dir().open(transaction, path).join();
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return CompletableFuture.completedFuture(Result.err(new NoNodeException(request.getPath())));
      } else {
        throw new RuntimeException(e);
      }
    }

    // we could be even more targeted and just fetch data
    CompletableFuture<FdbNode> fdbNode = fdbNodeReader.getNode(subspace, transaction);

    if (request.getWatch()) {
      fdbWatchManager.addNodeDataUpdatedWatch(transaction, request.getPath(), zkRequest.cnxn, zkRequest.sessionId);
      fdbWatchManager.addNodeDeletedWatch(transaction, request.getPath(), zkRequest.cnxn, zkRequest.sessionId);
    }

    return CompletableFuture.completedFuture(Result.ok(new GetDataResponse(fdbNode.join().getData(), fdbNode.join().getStat())));
  }

}
