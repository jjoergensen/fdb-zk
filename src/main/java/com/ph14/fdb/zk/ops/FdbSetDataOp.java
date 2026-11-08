package com.ph14.fdb.zk.ops;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.zookeeper.KeeperException.APIErrorException;
import org.apache.zookeeper.KeeperException.BadVersionException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.FdbTransactionState;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.FdbWatchManager;
import com.ph14.fdb.zk.layer.StatKey;

public class FdbSetDataOp implements FdbOp<SetDataRequest, SetDataResponse> {

  private final FdbNodeWriter fdbNodeWriter;
  private final FdbNodeReader fdbNodeReader;
  private final FdbWatchManager fdbWatchManager;

  @Inject
  public FdbSetDataOp(FdbNodeReader fdbNodeReader,
                      FdbNodeWriter fdbNodeWriter,
                      FdbWatchManager fdbWatchManager) {
    this.fdbNodeReader = fdbNodeReader;
    this.fdbNodeWriter = fdbNodeWriter;
    this.fdbWatchManager = fdbWatchManager;
  }

  @Override
  public CompletableFuture<Result<SetDataResponse, KeeperException>> execute(Request zkRequest, Transaction transaction, SetDataRequest request) {
    return CompletableFuture.completedFuture(prepare(zkRequest, transaction, request));
  }

  public Result<SetDataResponse, KeeperException> prepare(Request zkRequest, Transaction transaction, SetDataRequest request) {
    List<String> path = FdbPath.toFdbPath(request.getPath());

    final DirectorySubspace subspace;
    final Stat stat;
    try {
      subspace = com.ph14.fdb.zk.FdbZkKeyspace.dir().open(transaction, path).join();
      stat = fdbNodeReader.getNodeStat(subspace, transaction).join();

      if (request.getVersion() != -1 && stat.getVersion() != request.getVersion()) {
        return Result.err(new BadVersionException(request.getPath()));
      }
    } catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) {
        return Result.err(new NoNodeException(request.getPath()));
      } else {
        return Result.err(new APIErrorException());
      }
    }

    byte[] data = request.getData() == null ? new byte[0] : request.getData();
    fdbNodeWriter.writeData(transaction, subspace, data);

    long now = System.currentTimeMillis();
    fdbNodeWriter.writeStat(transaction, subspace, ImmutableMap.<StatKey, Long>builder()
        .put(StatKey.MZXID, FdbNodeWriter.ZXID_FLAG)
        .put(StatKey.MTIME, now)
        .put(StatKey.VERSION, stat.getVersion() + 1L)
        .put(StatKey.DATA_LENGTH, (long) data.length)
        .build());

    FdbTransactionState.of(transaction).eventAcls.put(request.getPath(), fdbNodeReader.readAcls(subspace, transaction).join());
    fdbWatchManager.triggerNodeUpdatedWatch(transaction, request.getPath());

    stat.setMtime(now);
    stat.setVersion(stat.getVersion() + 1);
    stat.setDataLength(data.length);

    stat.setMzxid(FdbTransactionState.of(transaction).position());
    return Result.ok(new SetDataResponse(stat));
  }

}
