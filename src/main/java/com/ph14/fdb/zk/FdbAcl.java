package com.ph14.fdb.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperServer;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.google.inject.Inject;
import com.ph14.fdb.zk.layer.FdbNodeReader;
import com.ph14.fdb.zk.layer.FdbNodeWriter;
import com.ph14.fdb.zk.layer.FdbPath;
import com.ph14.fdb.zk.layer.StatKey;

public class FdbAcl {
  private final ZooKeeperServer server;
  private final FdbNodeReader reader;
  private final FdbNodeWriter writer;

  @Inject
  public FdbAcl(ZooKeeperServer server, FdbNodeReader reader, FdbNodeWriter writer) {
    this.server = server;
    this.reader = reader;
    this.writer = writer;
  }

  public void authorize(Request request, Transaction tr, Record command) {
    String path;
    int permission;
    List<ACL> replacement = null;
    if (command instanceof CreateRequest) {
      CreateRequest create = (CreateRequest) command;
      create.setAcl(fixup(create.getPath(), request, create.getAcl()));
      replacement = create.getAcl();
      path = FdbPath.toZkParentPath(create.getPath());
      permission = Perms.CREATE;
    } else if (command instanceof DeleteRequest) {
      path = FdbPath.toZkParentPath(((DeleteRequest) command).getPath()); permission = Perms.DELETE;
    } else if (command instanceof SetDataRequest) {
      path = ((SetDataRequest) command).getPath(); permission = Perms.WRITE;
    } else if (command instanceof SetACLRequest) {
      SetACLRequest set = (SetACLRequest) command;
      set.setAcl(fixup(set.getPath(), request, set.getAcl()));
      replacement = set.getAcl();
      path = set.getPath(); permission = Perms.ADMIN;
    } else if (command instanceof GetACLRequest) {
      path = ((GetACLRequest) command).getPath(); permission = Perms.READ | Perms.ADMIN;
    } else if (command instanceof GetDataRequest) {
      path = ((GetDataRequest) command).getPath(); permission = Perms.READ;
    } else if (command instanceof GetChildrenRequest) {
      path = ((GetChildrenRequest) command).getPath(); permission = Perms.READ;
    } else if (command instanceof GetChildren2Request) {
      path = ((GetChildren2Request) command).getPath(); permission = Perms.READ;
    } else if (command instanceof CheckVersionRequest) {
      path = ((CheckVersionRequest) command).getPath(); permission = Perms.READ;
    } else if (command instanceof ExistsRequest) {
      path = ((ExistsRequest) command).getPath(); permission = Perms.READ;
      if (!FdbZkKeyspace.dir().exists(tr, FdbPath.toFdbPath(path)).join()) return;
    } else return;
    try {
      DirectorySubspace node = reader.getNodeDirectory(tr, path).join();
      server.checkACL(request.cnxn, reader.readAcls(node, tr).join(), permission,
          request.authInfo, path, replacement);
    } catch (KeeperException e) { throw new Failure(e); }
    catch (CompletionException e) {
      if (e.getCause() instanceof NoSuchDirectoryException) throw new Failure(new KeeperException.NoNodeException(path));
      throw e;
    }
  }

  private List<ACL> fixup(String path, Request request, List<ACL> acls) {
    try { return PrepRequestProcessor.fixupACL(path, request.authInfo, acls); }
    catch (KeeperException e) { throw new Failure(e); }
  }

  public SetACLResponse setAcl(Request request, Transaction tr, SetACLRequest command) {
    DirectorySubspace node = reader.getNodeDirectory(tr, command.getPath()).join();
    Stat stat = reader.getNodeStat(node, tr).join();
    if (command.getVersion() != -1 && command.getVersion() != stat.getAversion()) {
      throw new Failure(new KeeperException.BadVersionException(command.getPath()));
    }
    FdbTransactionState.of(tr).mutate(tr);
    writer.writeACLs(tr, node, command.getAcl());
    stat.setAversion(stat.getAversion() + 1);
    writer.writeStat(tr, node, java.util.Map.of(StatKey.AVERSION, (long) stat.getAversion()));
    return new SetACLResponse(stat);
  }

  public GetACLResponse getAcl(Request request, Transaction tr, GetACLRequest command) {
    DirectorySubspace node = reader.getNodeDirectory(tr, command.getPath()).join();
    List<ACL> acls = reader.readAcls(node, tr).join();
    try { server.checkACL(request.cnxn, acls, Perms.ADMIN, request.authInfo, command.getPath(), null); }
    catch (KeeperException.NoAuthException denied) {
      List<ACL> masked = new ArrayList<>();
      for (ACL acl : acls) {
        Id id = acl.getId();
        masked.add("digest".equals(id.getScheme())
            ? new ACL(acl.getPerms(), new Id("digest", id.getId().replaceAll(":.*", ":x"))) : acl);
      }
      acls = masked;
    }
    return new GetACLResponse(acls, reader.getNodeStat(node, tr).join());
  }

  public static class Failure extends RuntimeException {
    public final KeeperException error;
    public Failure(KeeperException error) { this.error = error; }
  }
}
