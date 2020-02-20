// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.sequoiadb;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.sequoiadb.base.ConfigOptions;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.datasource.ConnectStrategy;
import com.sequoiadb.datasource.DatasourceOptions;
import com.sequoiadb.datasource.SequoiadbDatasource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

import static org.janusgraph.diskstorage.Backend.*;
import static org.janusgraph.diskstorage.Backend.SYSTEM_TX_LOG_NAME;
import static org.janusgraph.diskstorage.configuration.ConfigOption.disallowEmpty;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;

public class SequoiadbStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {
    private static final Logger log = LoggerFactory.getLogger(SequoiadbStoreManager.class);
    public static final StandardSerializer serializer = new StandardSerializer();
    private static final SequoiadbStaticBufferChange sdbStaticBufferChange = new SequoiadbStaticBufferChange();
    private final Map<String, String> shortCLNameMap;
    private boolean isClose = false;


    public static final ConfigNamespace SEQUOIADB_NS =
        new ConfigNamespace(STORAGE_NS, "sequoiadb", "sequoiadb configuration options");


    public static final ConfigOption<String> SPACENAME =
        new ConfigOption<String>(SEQUOIADB_NS, "spacename",
            "sequoiadb collection space name",
            ConfigOption.Type.MASKABLE,  String.class,
            "janusgraph", disallowEmpty(String.class));

//    public static final ConfigOption<Boolean> BULKINSERT =
//        new ConfigOption<Boolean>(SEQUOIADB_NS, "bulkinsert",
//            "sequoiadb is bulk insert?",
//            ConfigOption.Type.MASKABLE, true);
//
//    public static final ConfigOption<Integer> BULKNUM =
//        new ConfigOption<Integer>(SEQUOIADB_NS, "bulknum",
//            "sequoiadb bulk insert number",
//            ConfigOption.Type.MASKABLE, 100, ConfigOption.positiveInt());

    private String csName = "";
//    private boolean bulkinsert;
//    private int bulknum;

    private final Map<String, SequoiadbValueStore> stores;

//    private SequoiadbDatasource ds = null;

    public SequoiadbStoreManager(Configuration storageConfig) throws BackendException {
        super(storageConfig, SequoiadbStoreConfig.DEFAULTPORT);
        shortCLNameMap = createShortCfMap(storageConfig);

        stores = new HashMap<String, SequoiadbValueStore>();
        csName = storageConfig.get(SPACENAME);
//        bulkinsert = storageConfig.get(BULKINSERT);
//        bulknum = storageConfig.get(BULKNUM);

//        ds = SequoiadbHelper.initSequoiadbDs(hostnames,
//            port,
//            username,
//            password);
    }
    @Override
    public Deployment getDeployment() {
        return Deployment.REMOTE;
    }

//    @Override
//    public SequoiadbValueStore openDatabase(String name) throws BackendException {
//        Preconditions.checkNotNull(name);
//        if (stores.containsKey(name)) {
//            return stores.get(name);
//        }
//
//
//        Sequoiadb conn = null;
//        try {
//            conn = this.ds.getConnection();
//        } catch (InterruptedException e) {
//            throw new PermanentBackendException("get Sequoiadb conn fail, error = " + e.getMessage());
//        }
//        String csName = this.csName;
//        String clName = name;
//
//        SequoiadbValueStore store = new SequoiadbValueStore(conn, csName, clName, this);
//        stores.put(name, store);
//        return store;
//    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        String clName = name;
        String shortClName = shortenCfName (shortCLNameMap, clName);

        SequoiadbValueStore store = stores.get(shortClName);

        if (store == null) {
            Sequoiadb conn = getConnect();

//            try {
//                conn = ds.getConnection();
//
//            } catch (InterruptedException e) {
//                throw new PermanentBackendException("get Sequoiadb conn fail, error = " + e.getMessage());
//            }

            store = new SequoiadbValueStore(conn, csName, shortClName, this);
            stores.put (shortClName, store);
        }
        return store;
    }


    // key in record
    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        List<BSONObject> deleteList = new ArrayList<BSONObject> ();
        List<BSONObject> updateList = new ArrayList<BSONObject> ();

        String clName = null;
        SequoiadbValueStore store = null;

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
            clName = entry.getKey();
            String shortClName = shortenCfName (shortCLNameMap, clName);

            store = stores.get(shortClName);
            if (store == null) {
                Sequoiadb conn = getConnect();
                char[] c = shortClName.toCharArray();
                log.info ("***");
                for (char a : c) {
                    log.info ("char = " + a + ", ascii = " + Integer.valueOf(a).intValue());
                }
                log.info ("***");
                log.info ("manager.mutateMany shortClName = " + shortClName + ", longClName = " + clName);
                store = new SequoiadbValueStore(conn, csName, shortClName, this);
                stores.put (shortClName, store);
            }

            DBCollection CL = store.getCL();

            BSONObject deleteObj = null;
            BSONObject updateObj = null;
            BSONObject updateQueryObj = null;
            BSONObject tmpObj = null;


            for (Map.Entry<StaticBuffer, KCVMutation> m : entry.getValue().entrySet()) {
                String rowKey_str = sdbStaticBufferChange.toStringBinary(m.getKey().as(StaticBuffer.ARRAY_FACTORY));
//                Binary rowKey = new Binary (m.getKey().as(StaticBuffer.ARRAY_FACTORY));
                Binary rowKey = sdbStaticBufferChange.staticBuffer2Binary(m.getKey());

//                tmpObj = new BasicBSONObject ();
//                tmpObj.put ("RowKey", rowKey);
//                CL.insert(tmpObj);

                KCVMutation mutation = m.getValue();

                if (mutation.hasDeletions()) {

                    for (StaticBuffer b : mutation.getDeletions()) {
                        deleteObj = new BasicBSONObject();
                        deleteObj.put ("RowKey", rowKey);
//                        deleteObj.put ("Key", sdbStaticBufferChange.staticBuffer2String(b));
                        deleteObj.put ("Key", sdbStaticBufferChange.staticBuffer2Binary(b));

                        deleteList.add (deleteObj);
                    }
                }

                if (mutation.hasAdditions()) {

                    for (Entry e : mutation.getAdditions()) {
//                    for (StaticBuffer e : mutation.getAdditions()) {
                        updateObj = new BasicBSONObject();
                        tmpObj = new BasicBSONObject();

                        updateObj.put ("RowKey", rowKey);

                        Object key_str = sdbStaticBufferChange.staticBuffer2String(e.getColumnAs(StaticBuffer.STATIC_FACTORY));
//                        Object key_str = sdbStaticBufferChange.staticBuffer2String(e);

                        Object key = sdbStaticBufferChange.staticBuffer2Binary(e.getColumnAs(StaticBuffer.STATIC_FACTORY));
                        Object value = sdbStaticBufferChange.staticBuffer2Binary(e.getValueAs(StaticBuffer.STATIC_FACTORY));
//                        Object value_str = sdbStaticBufferChange.staticBuffer2Object(e.getValueAs(StaticBuffer.STATIC_FACTORY), Object.class);
//                        Object key = sdbStaticBufferChange.staticBuffer2Binary(e);
//                        Object value = sdbStaticBufferChange.staticBuffer2Binary(e);

                        tmpObj.put ("Key_str", key_str);
                        tmpObj.put ("Key", key);
                        tmpObj.put ("Value", value);
//                        tmpObj.put ("Value_str", value_str);
                        tmpObj.put ("RowKey_str", rowKey_str);

                        updateObj.put ("$set", tmpObj);

                        updateList.add (updateObj);

                    }
                }

                if (deleteList.size() != 0) {
                    for (BSONObject dObj : deleteList) {
                        CL.delete(dObj);
                    }
                }

                if (updateList.size() != 0) {
                    for (BSONObject uObj : updateList) {
                        updateQueryObj = new BasicBSONObject();

                        updateQueryObj.put ("RowKey", uObj.removeField("RowKey"));
                        CL.upsert(updateQueryObj, uObj, null);
                    }
                }
            }
        }
        if (isClose) {
            store.getConn().close();
        }
    }

    // key in Array
//    @Override
//    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
//
//        Boolean haveUpdateDeleteExec = false;
//        Boolean haveUpdateAddExec = false;
//        String clName = null;
//
//        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
//            clName = entry.getKey();
//
//            SequoiadbValueStore store = stores.get(clName);
//
//            DBCollection CL = store.getCL();
//
//            BSONObject queryObj = new BasicBSONObject();
//            BSONObject updateDeleteObj = new BasicBSONObject();
//            BSONObject updateAddObj = new BasicBSONObject();
//
//            for (Map.Entry<StaticBuffer, KCVMutation> m : entry.getValue().entrySet()) {
////                final byte[] key = m.getKey().as(StaticBuffer.ARRAY_FACTORY);
//                String rowKey = sdbStaticBufferChange.staticBuffer2String(m.getKey());
//                KCVMutation mutation = m.getValue();
//                queryObj.put ("RowKey", rowKey);
//
//                BasicBSONList tDeleteList = new BasicBSONList();
//                BasicBSONList tAddList = new BasicBSONList();
//
//                BSONObject tmpDelete1Obj = new BasicBSONObject();
//                BSONObject tmpAdd1Obj = new BasicBSONObject();
//
//                if (mutation.hasDeletions()) {
//                    haveUpdateDeleteExec = true;
//
//                    for (StaticBuffer b : mutation.getDeletions()) {
//                        BSONObject tmp2Obj = new BasicBSONObject();
//                        tmp2Obj.put ("Key", sdbStaticBufferChange.staticBuffer2String(b));
//                        tDeleteList.add (tmp2Obj);
//                    }
//                }
//
//                if (mutation.hasAdditions()) {
//                    haveUpdateDeleteExec = true;
//                    haveUpdateAddExec = true;
//
//                    for (Entry e : mutation.getAdditions()) {
//                        Object key = sdbStaticBufferChange.staticBuffer2String(e.getColumnAs(StaticBuffer.STATIC_FACTORY));
//
//                        Object value = sdbStaticBufferChange.staticBuffer2Object(e.getValueAs(StaticBuffer.STATIC_FACTORY), Object.class);
//
//                        BSONObject tmp2DeleteObj = new BasicBSONObject();
//                        tmp2DeleteObj.put ("Key", key);
//                        tDeleteList.add (tmp2DeleteObj);
//
//                        BSONObject tmp2AddObj = new BasicBSONObject();
//                        tmp2AddObj.put ("Key", key);
//                        tmp2AddObj.put ("Value", value);
//                        tAddList.add (tmp2AddObj);
//                    }
//                }
//
//                if (haveUpdateDeleteExec) {
//                    tmpDelete1Obj.put("Element", tDeleteList);
//                    updateDeleteObj.put("$pull_all_by", tmpDelete1Obj);
//
//                    CL.upsert(queryObj, updateDeleteObj, null);
//                }
//
//                if (haveUpdateAddExec) {
//                    tmpAdd1Obj.put("Element", tAddList);
//                    updateAddObj.put("$addtoset", tmpAdd1Obj);
//                    CL.upsert(queryObj, updateAddObj, null);
//                }
//            }
//        }
//    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        SequoiadbTx sTx = new SequoiadbTx(config);
        return sTx;
    }

    @Override
    public void close() throws BackendException {
//        if (! stores.isEmpty()) {
//            for (Map.Entry<String, SequoiadbValueStore> store : stores.entrySet()) {
//                SequoiadbValueStore s = store.getValue();
//                this.ds.releaseConnection(s.getConn());
//            }
//        }
//        this.ds.close();
        isClose = true;
    }

    @Override
    public void clearStorage() throws BackendException {
        if (! stores.isEmpty()) {
            for (Map.Entry<String, SequoiadbValueStore> store : stores.entrySet()) {
                SequoiadbValueStore s = store.getValue();
                if (this.storageConfig.get(DROP_ON_CLEAR)) {
                    s.dropCl();
                } else {
                    s.clearCl();
                }
            }
        }
    }

    @Override
    public boolean exists() throws BackendException {
        return true;
    }

    @Override
    public StoreFeatures getFeatures() {

//        Configuration c = GraphDatabaseConfiguration.buildGraphConfiguration();

//        StandardStoreFeatures.Builder fb2 = new StandardStoreFeatures.Builder()
//            .orderedScan(true)
//            .unorderedScan(true)
//            .batchMutation(true)
//            .multiQuery(true)
//            .distributed(true)
//            .keyOrdered(true)
//            .storeTTL(true)
//            .cellTTL(true)
//            .timestamps(true)
//            .preferredTimestamps(PREFERRED_TIMESTAMPS)
//            .optimisticLocking(true).keyConsistent(c);

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder()
            .timestamps(false)
            .storeTTL(false)
            .cellTTL(false)
            .visibility(true)
            .orderedScan(true)
            .unorderedScan(true)
//            .transactional(transactional)
//            .transactional(false)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
//            .multiQuery(true)
            .locking(true)
            .keyOrdered(true)
            .distributed(true)
//            .scanTxConfig()
            .supportsInterruption(false)
            .optimisticLocking(false)
            .localKeyPartition(false);


        return fb.build();
    }

    @Override
    public String getName() {
        return csName;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }



//    public SequoiadbDatasource getSequoaidbDs () {
//        return this.ds;
//    }

//    public static BiMap<String, String> createShortCfMap(Configuration config) {
//        return ImmutableBiMap.<String, String>builder()
//            .put(INDEXSTORE_NAME, "g")
//            .put(INDEXSTORE_NAME + LOCK_STORE_SUFFIX, "h")
//            .put(config.get(IDS_STORE_NAME), "i")
//            .put(EDGESTORE_NAME, "e")
//            .put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, "f")
//            .put(SYSTEM_PROPERTIES_STORE_NAME, "s")
//            .put(SYSTEM_PROPERTIES_STORE_NAME + LOCK_STORE_SUFFIX, "t")
//            .put(SYSTEM_MGMT_LOG_NAME, "m")
//            .put(SYSTEM_TX_LOG_NAME, "l")
//            .put("janusgraph:e", "e2")
//            .build();
//    }
    public static Map<String, String> createShortCfMap (Configuration config) {
        Map<String, String> map = new HashMap<String, String>();
        map.put(INDEXSTORE_NAME, "g");
        map.put(INDEXSTORE_NAME + LOCK_STORE_SUFFIX, "h");
        map.put(config.get(IDS_STORE_NAME), "i");
        map.put(EDGESTORE_NAME, "e");
        map.put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, "f");
        map.put(SYSTEM_PROPERTIES_STORE_NAME, "s");
        map.put(SYSTEM_PROPERTIES_STORE_NAME + LOCK_STORE_SUFFIX, "t");
        map.put(SYSTEM_MGMT_LOG_NAME, "m");
        map.put(SYSTEM_TX_LOG_NAME, "l");
        map.put("janusgraph:g", "g");
        map.put("janusgraph:h", "h");
        map.put("janusgraph:i", "i");
        map.put("janusgraph:e", "e");
        map.put("janusgraph:f", "f");
        map.put("janusgraph:s", "s");
        map.put("janusgraph:t", "t");
        map.put("janusgraph:m", "m");
        map.put("janusgraph:l", "l");

        return map;
    }

    public static String shortenCfName(Map<String, String> shortCLNameMap, String longName) throws PermanentBackendException {
        final String s;
        if (shortCLNameMap.containsKey(longName)) {
            s = shortCLNameMap.get(longName);
            Preconditions.checkNotNull(s);
            log.debug("Substituted default CL name \"{}\" with short form \"{}\" to reduce SequoiaDB KeyValue size", longName, s);
        } else {
//            if (shortCLNameMap.containsValue(longName)) {
//                String fmt = "Must use CL long-form name \"%s\" instead of the short-form name \"%s\" when configured with %s=true";
//                String msg = String.format(fmt, shortCLNameMap.inverse().get(longName), longName, SHORT_CF_NAMES.getName());
//                throw new PermanentBackendException(msg);
//            }
            s = longName;
            log.debug("Kept default CL name \"{}\" because it has no associated short form", s);
        }
        return s;
    }

    private Sequoiadb getConnect () {
        Sequoiadb conn = null;

        List<String> addrs = new ArrayList<String>();

        for (String h : hostnames) {
            addrs.add (h + ":" + port);
        }
        ConfigOptions options = null;
        conn = new Sequoiadb (addrs,
            username,
            password,
            options);

        return conn;
    }

}
