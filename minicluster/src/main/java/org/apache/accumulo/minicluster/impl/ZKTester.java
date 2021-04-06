package org.apache.accumulo.minicluster.impl;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;

public class ZKTester extends ZooKeeper {

    public ZKTester(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
        super(connectString, sessionTimeout, watcher);
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
        return super.create(path, data, acl, createMode);
    }

    @Override
    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
        super.create(path, data, acl, createMode, cb, ctx);
    }

    @Override
    public void delete(String path, int version) throws InterruptedException, KeeperException {
        super.delete(path, version);
    }
}
