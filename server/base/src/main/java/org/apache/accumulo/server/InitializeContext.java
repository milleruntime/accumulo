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
package org.apache.accumulo.server;

import java.io.IOException;
import java.util.UUID;

import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitializeContext implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(InitializeContext.class);

  private VolumeManager fs;
  private SiteConfiguration siteConf;
  private Configuration hadoopConf;

  private String instanceName;

  public InitializeContext(SiteConfiguration siteConf, Configuration hadoopConf)
      throws IOException {
    this.siteConf = siteConf;
    this.hadoopConf = hadoopConf;
    this.fs = VolumeManagerImpl.get(siteConf, hadoopConf);
  }

  public VolumeManager getVolumeManager() {
    return this.fs;
  }

  public boolean isInitialized() {
    try {
      for (String baseDir : VolumeConfiguration.getVolumeUris(siteConf, hadoopConf)) {
        if (fs.exists(new Path(baseDir, ServerConstants.INSTANCE_ID_DIR))
                || fs.exists(new Path(baseDir, ServerConstants.VERSION_DIR))) {
          return true;
        }
      }
    } catch (IOException e) {
      log.error("Error checking if Accumulo is initialized", e);
    }
    return false;
  }

  public void initDirs(UUID uuid, String[] baseDirs, boolean print) throws IOException {
    for (String baseDir : baseDirs) {
      fs.mkdirs(new Path(new Path(baseDir, ServerConstants.VERSION_DIR),
              "" + ServerConstants.DATA_VERSION), new FsPermission("700"));

      // create an instance id
      Path iidLocation = new Path(baseDir, ServerConstants.INSTANCE_ID_DIR);
      fs.mkdirs(iidLocation);
      fs.createNewFile(new Path(iidLocation, uuid.toString()));
      if (print) {
        log.info("Initialized volume {}", baseDir);
      }
    }
  }

  public SiteConfiguration getSiteConf() {
    return siteConf;
  }

  public void setSiteConf(SiteConfiguration siteConf) {
    this.siteConf = siteConf;
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  public void setHadoopConf(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  @Override
  public void close() throws Exception {
    //TODO anything to wrap up?
  }
}
