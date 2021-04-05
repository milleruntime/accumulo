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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 *
 */
public class MultipleTserverTabletsIT extends ConfigurableMacBase {

    @Override
    public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
        cfg.setSiteConfig(Collections.singletonMap(Property.GENERAL_RPC_TIMEOUT.getKey(), "2s"));
    }

    @Override
    protected int defaultTimeoutSeconds() {
        return 3 * 60;
    }

    @Test
    public void test() throws Exception {
        Process zombie = cluster.exec(LivingDeadTServer.class);
        final Connector c = getConnector();
        final String tableName = getUniqueNames(1)[0];
        c.tableOperations().create(tableName);
        c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "1.0");
        FileSystem fs = cluster.getFileSystem();
        Path root = new Path(cluster.getTemporaryPath(), getClass().getName());
        Path testrf = new Path(root, "testrf");
        FunctionalTestUtils.createRFiles(c, fs, testrf.toString(), 500000, 59, 4);
        int beforeCount = countFiles(c);

        FunctionalTestUtils.bulkImport(c, fs, tableName, testrf.toString());
        VerifyIngest.Opts opts = new VerifyIngest.Opts();
        String adminPass = cluster.getConfig().getRootPassword();
        String admin = cluster.getConfig().getRootUserName();
        final int span = 500000 / 59;
        for (int i = 0; i < 500000; i += 500000 / 59) {
            opts.startRow = i;
            opts.rows = span;
            opts.random = 56;
            opts.dataSize = 50;
            opts.cols = 1;
            opts.setTableName(tableName);
            opts.setPassword(new ClientOpts.Password(adminPass));
            opts.setPrincipal(admin);
            VerifyIngest.verifyIngest(c, opts, new ScannerOpts());
        }

        c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

        int finalCount = countFiles(c);

        assertEquals(0, zombie.waitFor());
        assertTrue(finalCount < beforeCount);
    }

    private int countFiles(Connector c) throws Exception {
        Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        s.fetchColumnFamily(new Text(MetadataSchema.TabletsSection.TabletColumnFamily.NAME));
        s.fetchColumnFamily(new Text(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME));
        return Iterators.size(s.iterator());
    }

}

