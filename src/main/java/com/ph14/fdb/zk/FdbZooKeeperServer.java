package com.ph14.fdb.zk;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.OptionalLong;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.SessionTracker.Session;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.ph14.fdb.zk.config.FdbZooKeeperModule;
import com.ph14.fdb.zk.layer.FdbNode;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.changefeed.SessionWatchFeed;
import com.ph14.fdb.zk.session.FdbSessionClock;
import com.ph14.fdb.zk.session.FdbSessionDataPurger;
import com.ph14.fdb.zk.session.FdbSessionManager;

public class FdbZooKeeperServer extends ZooKeeperServer {

  private static final Logger LOG = LoggerFactory.getLogger(FdbZooKeeperServer.class);
  private SessionWatchFeed sessionWatchFeed;

  public FdbZooKeeperServer(int tickTime) throws IOException {
    super(null, null, tickTime);
  }

  @Override
  public void startup() {
    System.out.println("Starting up the server");

    try (Database fdb = FDB.selectAPIVersion(630).open()) {
      com.ph14.fdb.zk.FdbZkKeyspace.init(fdb);
      fdb.run(tr -> {
        boolean rootNodeAlreadyExists = com.ph14.fdb.zk.FdbZkKeyspace.dir().exists(tr, Collections.singletonList(FdbPath.ROOT_PATH)).join();

        if (!rootNodeAlreadyExists) {
          LOG.info("Creating root path '/'");
          DirectorySubspace rootSubspace = com.ph14.fdb.zk.FdbZkKeyspace.dir().create(tr, Collections.singletonList(FdbPath.ROOT_PATH)).join();
          new FdbNodeWriter().createNewNode(tr, rootSubspace, new FdbNode("/", null, new byte[0], Ids.OPEN_ACL_UNSAFE));
        }

        return null;
      });
    }

    getZKDatabase().setlastProcessedZxid(Long.MAX_VALUE);

    super.startup();
  }

  @Override
  protected void createSessionTracker() {
    sessionTracker = new FdbSessionManager(FDB.selectAPIVersion(630).open(), Clock.systemUTC(), OptionalLong.empty());
  }

  @Override
  protected void startSessionTracker() {
  }

  @Override
  public void expire(Session session) {
    LOG.debug("Expiring session: {}", session);
    super.expire(session);
  }

  @Override
  public void loadData() {
  }

  @Override
  protected void killSession(long sessionId, long zxid) {
  }

  @Override
  protected void setupRequestProcessors() {
    // No super call: ZooKeeper 3.9 would start its own Prep/Sync/Final pipeline (txn log, in-memory DataTree).
    // FoundationDB is the store, so every request goes to FdbRequestProcessor only.

    Injector injector = Guice.createInjector(new FdbZooKeeperModule());

    FdbZooKeeperImpl fdbZooKeeper = injector.getInstance(FdbZooKeeperImpl.class);
    sessionWatchFeed = injector.getInstance(SessionWatchFeed.class);

    FdbSessionClock fdbSessionClock = new FdbSessionClock(
        injector.getInstance(Database.class),
        Clock.systemUTC(),
        OptionalLong.empty(),
        (FdbSessionManager) sessionTracker,
        injector.getInstance(FdbSessionDataPurger.class));

    // if this is the only node connecting, we want to clear ephemerals before allowing requests through
    fdbSessionClock.runOnce();
    fdbSessionClock.run();

    this.firstProcessor = new FdbRequestProcessor(null, this, fdbZooKeeper, sessionWatchFeed);
  }

  @Override
  public void finishSessionInit(ServerCnxn cnxn, boolean valid) {
    Runnable startWatches = () -> {};
    if (valid) {
      try {
        startWatches = sessionWatchFeed.connect(cnxn.getSessionId(), cnxn, () -> isConnected(cnxn));
      } catch (RuntimeException e) {
        LOG.error("Could not attach watches for session {}", Long.toHexString(cnxn.getSessionId()), e);
        valid = false;
      }
    }
    super.finishSessionInit(cnxn, valid);
    if (valid) {
      startWatches.run();
    }
  }

  private boolean isConnected(ServerCnxn cnxn) {
    ServerCnxnFactory factory = cnxn.isSecure() ? secureServerCnxnFactory : serverCnxnFactory;
    if (factory != null && !cnxn.isStale() && !cnxn.isInvalid()) {
      for (ServerCnxn connection : factory.getConnections()) {
        if (connection == cnxn) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public synchronized void shutdown(boolean fullyShutDown) {
    if (sessionWatchFeed != null) {
      sessionWatchFeed.shutdown();
    }
    super.shutdown(fullyShutDown);
  }

}
