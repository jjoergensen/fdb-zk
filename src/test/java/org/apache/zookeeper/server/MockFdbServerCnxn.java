package org.apache.zookeeper.server;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.jute.Record;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.ReplyHeader;

/** Test connection for ZooKeeper 3.9's ServerCnxn: records watch events, ignores everything else. */
public class MockFdbServerCnxn extends ServerCnxn {

  private final BlockingQueue<WatchedEvent> watchedEvents = new ArrayBlockingQueue<>(10);

  public MockFdbServerCnxn() {
    super(null);
  }

  @Override
  public int sendResponse(ReplyHeader h, Record r, String tag, String cacheKey, Stat stat, int opCode) {
    return 0;
  }

  @Override
  public void process(WatchedEvent event, List<ACL> znodeAcl) {
    watchedEvents.add(event);
  }

  @Override protected ServerStats serverStats() { return null; }
  @Override public long getOutstandingRequests() { return 0; }
  @Override public InetSocketAddress getRemoteSocketAddress() { return null; }
  @Override public int getInterestOps() { return 0; }
  @Override public boolean isSecure() { return false; }
  @Override public Certificate[] getClientCertificateChain() { return null; }
  @Override public void setClientCertificateChain(Certificate[] chain) { }
  @Override int getSessionTimeout() { return 0; }
  @Override public void close(DisconnectReason reason) { }
  @Override public void sendCloseSession() { }
  @Override public long getSessionId() { return 0; }
  @Override void setSessionId(long sessionId) { }
  @Override void sendBuffer(ByteBuffer... buffers) { }
  @Override void enableRecv() { }
  @Override void disableRecv(boolean waitDisableRecv) { }
  @Override void setSessionTimeout(int sessionTimeout) { }

  public BlockingQueue<WatchedEvent> getWatchedEvents() {
    return watchedEvents;
  }

  public void clearWatchedEvents() {
    watchedEvents.clear();
  }
}
