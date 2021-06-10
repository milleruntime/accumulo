/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.conf.Property.GC_CYCLE_DELAY;
import static org.apache.accumulo.core.conf.Property.GC_CYCLE_START;
import static org.apache.accumulo.core.conf.Property.TABLE_FILE_MAX;
import static org.apache.accumulo.core.conf.Property.TABLE_MAJC_RATIO;
import static org.apache.accumulo.core.conf.Property.TABLE_SPLIT_THRESHOLD;
import static org.apache.accumulo.core.conf.Property.TSERV_WALOG_MAX_SIZE;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.Opts;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MetadataGCBugIT extends ConfigurableMacBase {

  private static final int N = 1000; // number of rows per table
  private static final int COUNT = 5;
  private static final BatchWriterOpts BWOPTS = new BatchWriterOpts();
  private static final ScannerOpts SOPTS = new ScannerOpts();

  @Override
  protected int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration fsConf) {
    cfg.setProperty(TSERV_WALOG_MAX_SIZE, "1M");
    cfg.setProperty(TABLE_SPLIT_THRESHOLD, "10K");
    cfg.setProperty(TABLE_MAJC_RATIO, "1000");
    cfg.setProperty(TABLE_FILE_MAX, "1000");
    cfg.setProperty(GC_CYCLE_DELAY, "1s");
    cfg.setProperty(GC_CYCLE_START, "0s");
    cfg.setNumTservers(4);
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    FileSystem fs = getCluster().getFileSystem();
    Path basePath = getCluster().getTemporaryPath();
    String principal = getCluster().getConfig().getRootUserName();
    String filePrefix = this.getClass().getName();
    String dirSuffix = testName.getMethodName();
    String[] tableNames = getUniqueNames(2);
    AccumuloConfiguration aconf =
            new ServerConfigurationFactory(c.getInstance()).getConfiguration();

    // create lots of metadata tablets
    c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(), "50K");

    final SortedSet<Text> splits = getSplits();

    log.info("Creating {} tables", tableNames.length);
    for (String t : tableNames)
      bulkImport(c, t, fs, basePath, filePrefix, dirSuffix);
    //createTablesLiveIngest(c, tableNames, splits);

    //BulkFileIT.printRootTable(c, new Range(), "MetadataRecoveryBugIT_test");

    //Map<String,WalStateManager.WalState> wals = WALSunnyDayIT._getWals(c);
    //log.info("Number of WALS = " + wals.size());
    log.info("First key of {} = {}", tableNames[0], getFirstKey(c, tableNames[0]));

    log.info("Number of MD splits = " + c.tableOperations().listSplits(MetadataTable.NAME).size());

    //c.tableOperations().compact(MetadataTable.NAME, new CompactionConfig().setWait(true));
    //log.info("MD disk usage = " + c.tableOperations().getDiskUsage(Collections.singleton(MetadataTable.NAME)));
    //printMD(c, new Range(), "MetadataRecoveryBugIT_test");

    eliminateTabletServer();
    log.info("Killed tserver. Now wait for balance");
    c.instanceOperations().waitForBalance();
    //c.tableOperations().compact(MetadataTable.NAME, new CompactionConfig().setWait(true));
    //log.info("Compaction finished");
    log.info("restarting...");
    getCluster().getClusterControl().start(ServerType.TABLET_SERVER);

    printRootTable(c, new Range(), "MetadataRecoveryBugIT_test");

    Thread mdScanThread = new Thread(() -> {
      Scanner scan = null;
      try {
        scan = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        int num = c.tableOperations().listSplits(MetadataTable.NAME).size();
        while (num > 5) {
          int count = 0;
          log.info("Scanning MD...");
          for (Map.Entry<Key, Value> e : scan)
            count++;
          num = c.tableOperations().listSplits(MetadataTable.NAME).size();
          log.info("MD entries = {} splits = {}", count, num);
          UtilWaitThread.sleepUninterruptibly(10, TimeUnit.SECONDS);
        }
        log.info("Done Scanning MD");
      }catch (Exception e) {
        e.printStackTrace();
      }
    });
    mdScanThread.start();

    //verify(c, principal, tableName);

    batchDeleteMutations(c, tableNames, splits);

    for (String tableName : tableNames) {
      log.info("Delete table {}", tableName);
      c.tableOperations().delete(tableName);
    }
  }

  /**
   * Range of metadata to print. testName is the directory test name used to split file path string
   * since local fs paths are long and get truncated
   */
  public static void printMD(Connector c, Range range, String testName)
          throws TableNotFoundException {
    Scanner scanner = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    // MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
    // scanner.fetchColumnFamily(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME);
    // scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
    scanner.setRange(range);
    System.out.println("DUDE METADATA table " + range);
    for (Map.Entry<Key,Value> e : scanner) {
      System.out.println("" + e.getKey() + " " + e.getValue());
      // some columns will get truncated so print again, but just the end of the string
      if (e.getKey().getColumnFamily()
              .equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
        String[] q = e.getKey().getColumnQualifier().toString().split(testName);
        System.out.println("" + q[q.length - 1]);
      } else if (e.getKey().getRow().toString()
              .startsWith(MetadataSchema.DeletesSection.getRowPrefix())) {
        String[] q = e.getKey().getRow().toString().split(testName);
        System.out.println(MetadataSchema.DeletesSection.getRowPrefix() + q[q.length - 1]);
      }
    }
  }

  public static void printRootTable(Connector c, Range range, String testName)
          throws TableNotFoundException {
    Scanner scanner = c.createScanner(RootTable.NAME, Authorizations.EMPTY);
    // MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
    // scanner.fetchColumnFamily(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME);
    // scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
    scanner.setRange(range);
    System.out.println("DUDE ROOT table " + range);
    for (Map.Entry<Key,Value> e : scanner) {
      System.out.println("" + e.getKey() + " " + e.getValue());
      // some columns will get truncated so print again, but just the end of the string
      if (e.getKey().getColumnFamily()
              .equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
        String[] q = e.getKey().getColumnQualifier().toString().split(testName);
        System.out.println("" + q[q.length - 1]);
      } else if (e.getKey().getRow().toString()
              .startsWith(MetadataSchema.DeletesSection.getRowPrefix())) {
        String[] q = e.getKey().getRow().toString().split(testName);
        System.out.println(MetadataSchema.DeletesSection.getRowPrefix() + q[q.length - 1]);
      }
    }
  }

  private Key getFirstKey(Connector c, String tableName) throws Exception {
    Scanner scanner = c.createScanner(tableName, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> e : scanner){
      return e.getKey();
    }
    throw new Exception("No keys in " + tableName);
  }

  private void batchDeleteMutations(Connector c, String[] tableNames, SortedSet<Text> splits) throws Exception {
    for (String tableName : tableNames) {
      BatchDeleter bd = c.createBatchDeleter(tableName, Authorizations.EMPTY, 1, new BatchWriterConfig());
      log.info("Batch delete data for {}", tableName);
      for (Text s : splits) {
        Range r = new Range(new Text(s));
        bd.setRanges(Collections.singleton(r));
        bd.delete();
      }
      bd.close();
    }
  }

  private void createTablesLiveIngest(Connector c, String[] tableNames, SortedSet<Text> splits) throws Exception{
    for (String tableName : tableNames) {
      log.info("Creating " + tableName);
      // bulkImport(c, tableName, fs, basePath, filePrefix, dirSuffix);
      NewTableConfiguration ntc = new NewTableConfiguration();
      c.tableOperations().create(tableName, ntc);
      c.tableOperations().addSplits(tableName, splits);
      //write some data
      BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
      for (Text split : splits) {
        for (String s : "a b c d e f".split(" ")) {
          Mutation m = new Mutation(new Text(split + "_" + s));
          m.put(new Text("cf"), new Text(s), new Value(s.getBytes(StandardCharsets.UTF_8)));
          bw.addMutation(m);
        }
      }
      bw.close();
      log.info("Finished creating {} with {} splits", tableName, c.tableOperations()
              .listSplits(tableName).size());
    }
  }

  private SortedSet<Text> getSplits() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    final SortedSet<Text> splits = new TreeSet<>();
    Calendar cal = Calendar.getInstance();

    for (int j = 1; j < 1000; j++) {
      cal.add(Calendar.DAY_OF_MONTH, -1);
      Text s = new Text(sdf.format(cal.getTime()));
      splits.add(s);
    }
    return splits;
  }

  private void bulkImport(Connector c, String tableName, FileSystem fs,
                          Path basePath, String filePrefix, String dirSuffix) throws Exception {
    Path base = new Path(basePath, "testBulkFail_" + dirSuffix);
    fs.delete(base, true);
    fs.mkdirs(base);
    Path bulkFailures = new Path(base, "failures");
    Path files = new Path(base, "files");
    fs.mkdirs(bulkFailures);
    fs.mkdirs(files);

    Opts opts = new Opts();
    opts.timestamp = 1;
    opts.random = 56;
    opts.rows = N;
    opts.instance = c.getInstance().getInstanceName();
    opts.cols = 1;
    opts.setTableName(tableName);
    opts.conf = new Configuration(false);
    opts.fs = fs;
    opts.numsplits = 999;
    opts.createTable = true;
    String fileFormat = filePrefix + "rf%02d";
    for (int i = 0; i < COUNT; i++) {
      opts.outputFile = new Path(files, String.format(fileFormat, i)).toString();
      opts.startRow = N * i;
      TestIngest.ingest(c, fs, opts, BWOPTS);
    }
    opts.outputFile = new Path(files, String.format(fileFormat, N)).toString();
    opts.startRow = N;
    opts.rows = 1;
    // create an rfile with one entry, there was a bug with this:
    TestIngest.ingest(c, fs, opts, BWOPTS);

    log.info("Bulk import {} files to {}", COUNT, tableName);
    c.tableOperations().importDirectory(tableName, files.toString(), bulkFailures.toString(),
            false);
  }

  private void verify(Connector c, String principal, String tableName) throws Exception {
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.setTableName(tableName);
    vopts.random = 56;
    vopts.setPrincipal(principal);
    for (int i = 0; i < COUNT; i++) {
      vopts.startRow = i * N;
      vopts.rows = N;
      VerifyIngest.verifyIngest(c, vopts, SOPTS);
    }
    vopts.startRow = N;
    vopts.rows = 1;
    VerifyIngest.verifyIngest(c, vopts, SOPTS);
  }

  public void eliminateTabletServer() throws Exception {
    List<ProcessReference> procs =
        new ArrayList<>(getCluster().getProcesses().get(ServerType.TABLET_SERVER));
    ProcessReference pr = procs.get(0);
    log.info("Crashing {}", pr.getProcess());
    getCluster().killProcess(ServerType.TABLET_SERVER, pr);
  }
}
