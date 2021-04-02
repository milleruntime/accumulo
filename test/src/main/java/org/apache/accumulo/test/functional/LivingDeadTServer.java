package org.apache.accumulo.test.functional;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Iface;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Processor;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.master.tableOps.UserCompactionConfig;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

public class LivingDeadTServer extends TabletServer {

    private final ThriftClientHandler tch;

    public LivingDeadTServer(AccumuloServerContext context, TransactionWatcher watcher,
                             ServerConfigurationFactory confFactory, VolumeManager fs) throws IOException {
        super(confFactory, fs);
        this.tch = new ThriftClientHandler(context, watcher);
    }

    public ThriftClientHandler getTch(){
        return tch;
    }

    private class ThriftClientHandler
            extends org.apache.accumulo.test.performance.NullTserver.ThriftClientHandler {

        int statusCount = 0;

        boolean halted = false;
        boolean doneCompacting = false;

        ThriftClientHandler(AccumuloServerContext context, TransactionWatcher watcher) {
            super(context, watcher);
        }

        @Override
        synchronized public void fastHalt(TInfo tinfo, TCredentials credentials, String lock) {
            halted = true;
            notifyAll();
        }

        @Override
        public TabletServerStatus getTabletServerStatus(TInfo tinfo, TCredentials credentials)
                throws ThriftSecurityException, TException {
            synchronized (this) {
                if (statusCount++ < 1) {
                    TabletServerStatus result = new TabletServerStatus();
                    result.tableMap = new HashMap<>();
                    return result;
                }
            }
            sleepUninterruptibly(Integer.MAX_VALUE, TimeUnit.DAYS);
            return null;
        }

        @Override
        synchronized public void halt(TInfo tinfo, TCredentials credentials, String lock)
                throws ThriftSecurityException, TException {
            halted = true;
            notifyAll();
        }

        @Override
        public void compact(TInfo tinfo, TCredentials credentials, String lock, String tableId, ByteBuffer startRow, ByteBuffer endRow) throws TException {
            KeyExtent ke =
                    new KeyExtent(tableId, ByteBufferUtil.toText(endRow), ByteBufferUtil.toText(startRow));
            log.info("LivingDead Compacting tablet " + ke);

            ArrayList<Tablet> tabletsToCompact = new ArrayList<>();
                for (Tablet tablet : getOnlineTablets()) {
                    if (ke.overlaps(tablet.getExtent()))
                        tabletsToCompact.add(tablet);
                }
            Pair<Long, UserCompactionConfig> compactionInfo = null;
            for (Tablet tablet : tabletsToCompact) {
                // all for the same table id, so only need to read
                // compaction id once
                if (compactionInfo == null)
                    try {
                        compactionInfo = tablet.getCompactionID();
                    } catch (KeeperException.NoNodeException e) {
                        log.info("Asked to compact table with no compaction id {} {}", ke, e.getMessage());
                        return;
                    }
                tablet.compactAll(compactionInfo.getFirst(), compactionInfo.getSecond());
            }

            //TODO set condition for compacting the test tablet
            doneCompacting = true;
        }

        @Override
        public void loadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent textent) throws TException {
            final KeyExtent extent = new KeyExtent(textent);
            log.info("LivingDead Loading tablet " + extent);
            AssignmentHandler ah = new AssignmentHandler(extent);
            ah.run();

            log.info("LivingDead now has {} online tablets.", getOnlineTablets().size());
        }

        @Override
        public List<TKeyExtent> bulkImport(TInfo tinfo, TCredentials credentials, long tid, Map<TKeyExtent, Map<String, MapFileInfo>> files, boolean setTime) {
            log.info("LivingDead bulk importing " + files);

            List<TKeyExtent> failures = new ArrayList<>();

            for (Map.Entry<TKeyExtent,Map<String,MapFileInfo>> entry : files.entrySet()) {
                TKeyExtent tke = entry.getKey();
                Map<String,MapFileInfo> fileMap = entry.getValue();
                Map<FileRef,MapFileInfo> fileRefMap = new HashMap<>();
                for (Map.Entry<String,MapFileInfo> mapping : fileMap.entrySet()) {
                    Path path = new Path(mapping.getKey());
                    FileSystem ns = getFileSystem().getVolumeByPath(path).getFileSystem();
                    path = ns.makeQualified(path);
                    fileRefMap.put(new FileRef(path.toString(), path), mapping.getValue());
                }

                Tablet importTablet = getOnlineTablet(new KeyExtent(tke));

                if (importTablet == null) {
                    failures.add(tke);
                } else {
                    try {
                        importTablet.importMapFiles(tid, fileRefMap, setTime);
                    } catch (IOException ioe) {
                        log.info("files {} not imported to {}: {}", fileMap.keySet(), new KeyExtent(tke),
                                ioe.getMessage());
                        failures.add(tke);
                    }
                }
            }
            return failures;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ZombieTServer.class);

    public static void main(String[] args) throws IOException {
        try {
            Random random = new Random(System.currentTimeMillis() % 1000);
            int port = random.nextInt(30000) + 2000;
            ServerConfigurationFactory scf = new ServerConfigurationFactory(HdfsZooInstance.getInstance());
            AccumuloServerContext context = new AccumuloServerContext(scf);

            TransactionWatcher watcher = new TransactionWatcher();
            LivingDeadTServer z = new LivingDeadTServer(context, watcher, scf, null);
            final ThriftClientHandler tch = z.getTch();
            Processor<Iface> processor = new Processor<>(tch);
            ServerAddress serverPort = TServerUtils.startTServer(context.getConfiguration(),
                    ThriftServerType.CUSTOM_HS_HA, processor, "LivingDeadTServer", "walking dead", 2, 1, 1000,
                    10 * 1024 * 1024, null, null, -1, HostAndPort.fromParts("0.0.0.0", port));

            String addressString = serverPort.address.toString();
            log.info("Starting LivingDeadTServer at " + addressString);
            String zPath =
                    ZooUtil.getRoot(context.getInstance()) + Constants.ZTSERVERS + "/" + addressString;
            ZooReaderWriter zoo = ZooReaderWriter.getInstance();
            zoo.putPersistentData(zPath, new byte[]{}, org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy.SKIP);

            ZooLock zlock = new ZooLock(zPath);

            LockWatcher lw = new LockWatcher() {
                @Override
                public void lostLock(final org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason reason) {
                    try {
                        tch.halt(Tracer.traceInfo(), null, null);
                    } catch (Exception ex) {
                        log.error("Exception", ex);
                        System.exit(1);
                    }
                }

                @Override
                public void unableToMonitorLockNode(Throwable e) {
                    try {
                        tch.halt(Tracer.traceInfo(), null, null);
                    } catch (Exception ex) {
                        log.error("Exception", ex);
                        System.exit(1);
                    }
                }
            };

            byte[] lockContent =
                    new ServerServices(addressString, ServerServices.Service.TSERV_CLIENT).toString().getBytes(UTF_8);
            if (zlock.tryLock(lw, lockContent)) {
                log.debug("Obtained tablet server lock " + zlock.getLockPath());
            }
            // modify metadata
            synchronized (tch) {
                while (!tch.halted && !tch.doneCompacting) {
                    tch.wait();
                }
            }
            System.exit(0);
        } catch (Exception e) {
            log.error("Error running LivingDeadTServer", e);
            System.exit(1);
        }
    }
}
