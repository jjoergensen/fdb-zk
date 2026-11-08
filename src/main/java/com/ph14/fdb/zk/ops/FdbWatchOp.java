package com.ph14.fdb.zk.ops;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.proto.AddWatchRequest;
import org.apache.zookeeper.proto.CheckWatchesRequest;
import org.apache.zookeeper.proto.ErrorResponse;
import org.apache.zookeeper.proto.RemoveWatchesRequest;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.FdbWatchManager;

public class FdbWatchOp implements FdbOp<AddWatchRequest, ErrorResponse> {

  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbWatchOp(FdbWatchManager fdbWatchManager) {
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public CompletableFuture<Result<ErrorResponse, KeeperException>> execute(Request zkRequest, Transaction transaction, AddWatchRequest request) {
    try {
      PathUtils.validatePath(request.getPath());
      if (request.getMode() != AddWatchMode.PERSISTENT.getMode()
          && request.getMode() != AddWatchMode.PERSISTENT_RECURSIVE.getMode()) {
        return CompletableFuture.completedFuture(Result.err(new KeeperException.BadArgumentsException(request.getPath())));
      }
    } catch (IllegalArgumentException e) {
      return CompletableFuture.completedFuture(Result.err(new KeeperException.BadArgumentsException(request.getPath())));
    }
    fdbWatchManager.addPersistentWatch(transaction, request.getPath(), zkRequest.sessionId, request.getMode());
    return CompletableFuture.completedFuture(Result.ok(new ErrorResponse(0)));
  }

  public CompletableFuture<Result<Boolean, KeeperException>> execute(Request zkRequest, Transaction transaction, CheckWatchesRequest request) {
    return checkWatch(zkRequest, transaction, request.getPath(), request.getType(), false);
  }

  public CompletableFuture<Result<Boolean, KeeperException>> execute(Request zkRequest, Transaction transaction, RemoveWatchesRequest request) {
    return checkWatch(zkRequest, transaction, request.getPath(), request.getType(), true);
  }

  private CompletableFuture<Result<Boolean, KeeperException>> checkWatch(Request request, Transaction transaction, String path, int type, boolean remove) {
    final WatcherType watcherType;
    try {
      PathUtils.validatePath(path);
      watcherType = Arrays.stream(WatcherType.values()).filter(value -> value.getIntValue() == type)
          .findFirst().orElseThrow(IllegalArgumentException::new);
    } catch (IllegalArgumentException e) {
      return CompletableFuture.completedFuture(Result.err(new KeeperException.BadArgumentsException(path)));
    }
    return fdbWatchManager.checkWatch(transaction, path, request.sessionId, watcherType, remove)
        .thenApply(found -> found ? Result.ok(true) : Result.err(new KeeperException.NoWatcherException(path)));
  }
}
