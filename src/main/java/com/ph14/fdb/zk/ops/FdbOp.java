package com.ph14.fdb.zk.ops;

import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.Request;

import com.apple.foundationdb.Transaction;
import com.hubspot.algebra.Result;

public interface FdbOp<REQ, RESP> {

  CompletableFuture<Result<RESP, KeeperException>> execute(Request zkRequest, Transaction transaction, REQ request);

  static RuntimeException unwrap(RuntimeException error) {
    Throwable cause = error;
    while (cause instanceof java.util.concurrent.CompletionException && cause.getCause() != null) cause = cause.getCause();
    return cause instanceof RuntimeException ? (RuntimeException) cause : error;
  }

}
