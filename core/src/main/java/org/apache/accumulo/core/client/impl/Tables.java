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
package org.apache.accumulo.core.client.impl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import java.util.function.BiConsumer;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tables {
  private static final Logger log = LoggerFactory.getLogger(Tables.class);

  public static final String VALID_NAME_REGEX = "^(\\w+\\.)?(\\w+)$";

  private static final AtomicLong cacheResetCount = new AtomicLong(0);

  private static ZooCache getZooCache(Instance instance) {
    return new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut());
  }

  private static SortedMap<String,String> getMap(Instance instance, boolean nameAsKey) {
    ZooCache zc = getZooCache(instance);

    List<String> tableIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    TreeMap<String,String> tableMap = new TreeMap<>();
    Map<String,String> namespaceIdToNameMap = new HashMap<>();

    for (String tableId : tableIds) {
      byte[] tableName = zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAME);
      byte[] nId = zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAMESPACE);
      String namespaceName = Namespaces.DEFAULT_NAMESPACE;
      // create fully qualified table name
      if (nId == null) {
        namespaceName = null;
      } else {
        String namespaceId = new String(nId, UTF_8);
        if (!namespaceId.equals(Namespaces.DEFAULT_NAMESPACE_ID.canonicalID())) {
          try {
            namespaceName = namespaceIdToNameMap.get(namespaceId);
            if (namespaceName == null) {
              namespaceName = Namespaces.getNamespaceName(instance, namespaceId);
              namespaceIdToNameMap.put(namespaceId, namespaceName);
            }
          } catch (NamespaceNotFoundException e) {
            log.error("Table (" + tableId + ") contains reference to namespace (" + namespaceId + ") that doesn't exist", e);
            continue;
          }
        }
      }
      if (tableName != null && namespaceName != null) {
        String tableNameStr = qualified(new String(tableName, UTF_8), namespaceName);
        if (nameAsKey)
          tableMap.put(tableNameStr, tableId);
        else
          tableMap.put(tableId, tableNameStr);
      }
    }

    return tableMap;
  }

  /**
   * Lookup table ID in ZK. Throw TableNotFoundException if not found. Also wraps NamespaceNotFoundException in TableNotFoundException if namespace is not
   * found.
   */
  public static Table.ID getTableId(Instance instance, String tableName) throws TableNotFoundException {
    try {
      return _getTableId(instance, tableName);
    } catch (NamespaceNotFoundException e) {
      throw new TableNotFoundException(tableName, e);
    }
  }

  /**
   * Lookup table ID in ZK. If not found, clears cache and tries again.
   */
  public static Table.ID _getTableId(Instance instance, String tableName) throws NamespaceNotFoundException, TableNotFoundException {
    Table.ID tableId = lookupTableId(instance, tableName);
    if (tableId == null) {
      // maybe the table exist, but the cache was not updated yet... so try to clear the cache and check again
      clearCache(instance);
      tableId = lookupTableId(instance, tableName);
      if (tableId == null) {
        String namespace = qualify(tableName).getFirst();
        if (Namespaces.getNameMap(instance).containsKey(namespace))
          throw new TableNotFoundException(null, tableName, null);
        else
          throw new NamespaceNotFoundException(null, namespace, null);
      }
    }
    return tableId;
  }

  /**
   * @deprecated Do not use - Not type safe, ambiguous String-String map. Use {@link #getNameMap(Instance)}
   */
  @Deprecated
  public static SortedMap<String,String> getNameToIdMap(Instance instance) {
    return getMap(instance, true);
  }

  /**
   * @deprecated TODO remove once new Monitor gets merged, only used by old servlets
   */
  @Deprecated
  public static SortedMap<String,String> getIdToNameMap(Instance instance) {
    return getMap(instance, false);
  }

  public static boolean exists(Instance instance, Table.ID tableId) {
    ZooCache zc = getZooCache(instance);
    List<String> tableIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    return tableIds.contains(tableId.canonicalID());
  }

  public static void clearCache(Instance instance) {
    cacheResetCount.incrementAndGet();
    getZooCache(instance).clear(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    getZooCache(instance).clear(ZooUtil.getRoot(instance) + Constants.ZNAMESPACES);
  }

  /**
   * Clears the zoo cache from instance/root/{PATH}
   *
   * @param instance
   *          The Accumulo Instance
   * @param zooPath
   *          A zookeeper path
   */
  public static void clearCacheByPath(Instance instance, final String zooPath) {

    String thePath;

    if (zooPath.startsWith("/")) {
      thePath = zooPath;
    } else {
      thePath = "/" + zooPath;
    }

    getZooCache(instance).clear(ZooUtil.getRoot(instance) + thePath);

  }

  /**
   * @deprecated TODO remove once new Monitor gets merged, only used by old servlets
   */
  @Deprecated
  public static String getPrintableTableNameFromId(Map<String,String> tidToNameMap, String tableId) {
    String tableName = tidToNameMap.get(tableId);
    return tableName == null ? "(ID:" + tableId + ")" : tableName;
  }

  public static String getPrintableTableInfoFromId(Instance instance, Table.ID tableId) {
    String tableName = null;
    try {
      tableName = getTableName(instance, tableId);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableName == null ? String.format("?(ID:%s)", tableId.canonicalID()) : String.format("%s(ID:%s)", tableName, tableId.canonicalID());
  }

  public static String getPrintableTableInfoFromName(Instance instance, String tableName) {
    Table.ID tableId = null;
    try {
      tableId = getTableId(instance, tableName);
    } catch (TableNotFoundException e) {
      // handled in the string formatting
    }
    return tableId == null ? String.format("%s(?)", tableName) : String.format("%s(ID:%s)", tableName, tableId.canonicalID());
  }

  public static TableState getTableState(Instance instance, Table.ID tableId) {
    return getTableState(instance, tableId, false);
  }

  /**
   * Get the current state of the table using the tableid. The boolean clearCache, if true will clear the table state in zookeeper before fetching the state.
   * Added with ACCUMULO-4574.
   *
   * @param instance
   *          the Accumulo instance.
   * @param tableId
   *          the table id
   * @param clearCachedState
   *          if true clear the table state in zookeeper before checking status
   * @return the table state.
   */
  public static TableState getTableState(Instance instance, Table.ID tableId, boolean clearCachedState) {

    String statePath = ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId.canonicalID() + Constants.ZTABLE_STATE;

    if (clearCachedState) {
      Tables.clearCacheByPath(instance, statePath);
    }

    ZooCache zc = getZooCache(instance);
    byte[] state = zc.get(statePath);
    if (state == null)
      return TableState.UNKNOWN;

    return TableState.valueOf(new String(state, UTF_8));

  }

  public static long getCacheResetCount() {
    return cacheResetCount.get();
  }

  public static String qualified(String tableName) {
    return qualified(tableName, Namespaces.DEFAULT_NAMESPACE);
  }

  public static String qualified(String tableName, String defaultNamespace) {
    Pair<String,String> qualifiedTableName = qualify(tableName, defaultNamespace);
    if (Namespaces.DEFAULT_NAMESPACE.equals(qualifiedTableName.getFirst()))
      return qualifiedTableName.getSecond();
    else
      return qualifiedTableName.toString("", ".", "");
  }

  public static Pair<String,String> qualify(String tableName) {
    return qualify(tableName, Namespaces.DEFAULT_NAMESPACE);
  }

  public static Pair<String,String> qualify(String tableName, String defaultNamespace) {
    if (!tableName.matches(VALID_NAME_REGEX))
      throw new IllegalArgumentException("Invalid table name '" + tableName + "'");
    if (MetadataTable.OLD_NAME.equals(tableName))
      tableName = MetadataTable.NAME;
    if (tableName.contains(".")) {
      String[] s = tableName.split("\\.", 2);
      return new Pair<>(s[0], s[1]);
    }
    return new Pair<>(defaultNamespace, tableName);
  }

  /**
   * Returns the namespace id for a given table ID.
   *
   * @param instance
   *          The Accumulo Instance
   * @param tableId
   *          The tableId
   * @return The namespace id which this table resides in.
   * @throws IllegalArgumentException
   *           if the table doesn't exist in ZooKeeper
   */
  public static Namespace.ID getNamespaceId(Instance instance, Table.ID tableId) throws TableNotFoundException {
    checkArgument(instance != null, "instance is null");
    checkArgument(tableId != null, "tableId is null");

    ZooCache zc = getZooCache(instance);
    byte[] n = zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAMESPACE);

    // We might get null out of ZooCache if this tableID doesn't exist
    if (null == n) {
      throw new TableNotFoundException(tableId.canonicalID(), null, null);
    }

    return new Namespace.ID(new String(n, UTF_8));
  }

  /**
   * Populate map with biConsumer. Key = ID, Value = name.
   */
  private static void populateMap(Instance instance, BiConsumer<String,String> biConsumer) {
    ZooCache zc = getZooCache(instance);
    List<String> tableIds = zc.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLES);
    Map<Namespace.ID,String> namespaceIdToNameMap = new HashMap<>();

    for (String tableId : tableIds) {
      byte[] tableName = zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAME);
      byte[] nId = zc.get(ZooUtil.getRoot(instance) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAMESPACE);
      String namespaceName = Namespaces.DEFAULT_NAMESPACE;
      // create fully qualified table name
      if (nId == null) {
        namespaceName = null;
      } else {
        Namespace.ID namespaceId = new Namespace.ID(new String(nId, UTF_8));
        if (!namespaceId.equals(Namespaces.DEFAULT_NAMESPACE_ID)) {
          try {
            namespaceName = namespaceIdToNameMap.get(namespaceId);
            if (namespaceName == null) {
              namespaceName = Namespaces.getNamespaceName(instance, namespaceId);
              namespaceIdToNameMap.put(namespaceId, namespaceName);
            }
          } catch (NamespaceNotFoundException e) {
            log.error("Table (" + tableId + ") contains reference to namespace (" + namespaceId + ") that doesn't exist", e);
            continue;
          }
        }
      }
      if (tableName != null && namespaceName != null) {
        String tableNameStr = qualified(new String(tableName, UTF_8), namespaceName);
        biConsumer.accept(tableId, tableNameStr);
      }
    }
  }

  public static SortedMap<Table.ID,String> getIdMap(Instance instance) {
    SortedMap<Table.ID,String> map = new TreeMap<>();
    populateMap(instance, (k, v) -> map.put(new Table.ID(k), v));
    return map;
  }

  public static SortedMap<String,Table.ID> getNameMap(Instance instance) {
    SortedMap<String,Table.ID> map = new TreeMap<>();
    populateMap(instance, (k, v) -> map.put(v, new Table.ID(k)));
    return map;
  }

  /**
   * Lookup the table name in ZK. Fail quietly, returning null if not found.
   */
  public static Table.ID lookupTableId(Instance instance, String tableName) {
    ArrayList<Table.ID> singleId = new ArrayList<>(1);
    populateMap(instance, (id, name) -> {
      if (name.equals(tableName))
        singleId.add(new Table.ID(id));
    });
    if (singleId.isEmpty())
      return null;
    else
      return singleId.get(0);
  }

  public static String getTableName(Instance instance, Table.ID tableId) throws TableNotFoundException {
    ArrayList<String> singleName = new ArrayList<>(1);
    populateMap(instance, (id, name) -> {
      if (id.equals(tableId.canonicalID()))
        singleName.add(name);
    });
    if (singleName.isEmpty())
      throw new TableNotFoundException(tableId.canonicalID(), null, null);

    return singleName.get(0);
  }

}
