package com.ph14.fdb.zk;

import org.apache.zookeeper.server.NIOServerCnxnFactory;

import java.net.InetSocketAddress;

/**
 * Standalone fdb-zk server: speaks the ZooKeeper wire protocol on FDB_ZK_BIND:FDB_ZK_PORT and keeps all state in
 * FoundationDB (partition fdb-zk/FDB_ZK_NAMESPACE). Instances are stateless; run several and list them all in ZK_HOST.
 */
public final class Main {
  public static void main(String[] args) throws Exception {
    if (args.length == 2 && args[0].equals("wipe")) {
      // Removes exactly one ZK cluster: the Directory Layer partition fdb-zk/<namespace>. Nothing else is touched.
      try (var db = com.apple.foundationdb.FDB.selectAPIVersion(630).open()) {
        boolean removed = db.run(tr -> com.apple.foundationdb.directory.DirectoryLayer.getDefault()
            .removeIfExists(tr, java.util.List.of("fdb-zk", args[1])).join());
        System.out.println((removed ? "removed" : "absent") + " partition fdb-zk/" + args[1]);
      }
      return;
    }
    String bind = System.getenv().getOrDefault("FDB_ZK_BIND", "127.0.0.1");
    int port = Integer.parseInt(System.getenv().getOrDefault("FDB_ZK_PORT", "2181"));
    int tickTime = Integer.parseInt(System.getenv().getOrDefault("FDB_ZK_TICK_MS", "2000"));
    var server = new FdbZooKeeperServer(tickTime);
    var factory = new NIOServerCnxnFactory();
    factory.configure(new InetSocketAddress(bind, port), 200);
    factory.startup(server);
    System.out.println("fdb-zk listening on " + bind + ":" + port + " namespace " + System.getenv().getOrDefault("FDB_ZK_NAMESPACE", "default"));
    factory.join();
  }
}
