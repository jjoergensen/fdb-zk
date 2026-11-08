package com.ph14.fdb.zk;

import java.io.IOException;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hubspot.algebra.Result;
import com.ph14.fdb.zk.layer.changefeed.SessionWatchFeed;

public class FdbRequestProcessor implements RequestProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(FdbRequestProcessor.class);

  private final FdbZooKeeperImpl fdbZooKeeper;
  private final ZooKeeperServer zooKeeperServer;
  private final RequestProcessor defaultRequestProcessor;
  private final SessionWatchFeed sessionWatchFeed;

  public FdbRequestProcessor(RequestProcessor defaultRequestProcessor,
                             ZooKeeperServer zooKeeperServer,
                             FdbZooKeeperImpl fdbZooKeeper,
                             SessionWatchFeed sessionWatchFeed) {
    this.defaultRequestProcessor = defaultRequestProcessor;
    this.zooKeeperServer = zooKeeperServer;
    this.fdbZooKeeper = fdbZooKeeper;
    this.sessionWatchFeed = sessionWatchFeed;
  }

  @Override
  public void processRequest(Request request) {
    sessionWatchFeed.submit(request.sessionId,
        () -> sessionWatchFeed.processRequest(request.sessionId, () -> processSessionRequest(request)));
  }

  private void processSessionRequest(Request request) {
    try {
      LOG.debug("Received request in Fdb Request Processor: {}", request);
      Result<?, KeeperException> result;
      try {
        result = fdbZooKeeper.handlesRequest(request)
            ? fdbZooKeeper.handle(request)
            : Result.err(new KeeperException.UnimplementedException());
      } catch (IOException e) {
        LOG.error("Failed to deserialize request {}", request, e);
        result = Result.err(new KeeperException.MarshallingErrorException());
      } catch (Throwable e) {
        // RequestThrottler must survive failures in an individual request.
        LOG.error("Request failed: {}", request, e);
        result = Result.err(new KeeperException.SystemErrorException());
      }
      if (request.zxid < 0) request.zxid = fdbZooKeeper.currentZxid();
      if (request.type != OpCode.closeSession) sessionWatchFeed.beforeReply(request.sessionId, request.zxid);
      sendResponse(request, result);
    } catch (Throwable replyFailure) {
      LOG.error("Could not send reply for {}", request, replyFailure);
    } finally {
      try {
        zooKeeperServer.decInProcess();
      } catch (Throwable e) {
        LOG.error("Could not decrement in-process count for {}", request, e);
      }
      try {
        zooKeeperServer.requestFinished(request);
      } catch (Throwable e) {
        LOG.error("Could not finish request {}", request, e);
      }
    }
  }

  private void sendResponse(Request request,
                            Result<?, KeeperException> fdbResult) {
    if (request.type == OpCode.createSession) {
      zooKeeperServer.finishSessionInit(request.cnxn, fdbResult.isOk());
      return;
    }

    Record result = null;
    int errorCode = Code.OK.intValue();

    if (fdbResult.isOk()) {
      Object o = fdbResult.unwrapOrElseThrow();

      if (o instanceof Record) {
        result = (Record) o;
      }

      // note: it's OK for result to be null in the case of some operations like setWatches

      errorCode = Code.OK.intValue();
    } else if (fdbResult.isErr()) {
      LOG.error("Error: ", fdbResult.unwrapErrOrElseThrow());
      result = null;
      errorCode = fdbResult.unwrapErrOrElseThrow().code().intValue();
    } else {
      LOG.error("What is going on send help. Request: {}", request);
    }

    ReplyHeader hdr = new ReplyHeader(request.cxid, request.zxid, errorCode);

    try {
      LOG.debug("Returning: {}", result);
      request.cnxn.sendResponse(hdr, result, "response");
    } catch (IOException e) {
      LOG.error("FIXMSG",e);
    }
  }

  public void shutdown() {

  }
}
