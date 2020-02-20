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

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.datasource.SequoiadbDatasource;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransaction;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.indexing.*;
import org.janusgraph.graphdb.query.JanusGraphPredicate;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.janusgraph.diskstorage.configuration.ConfigOption.disallowEmpty;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NS;

public class SequoiadbIndex implements IndexProvider {

    public static final ConfigNamespace SEQUOIADB_NS =
        new ConfigNamespace(INDEX_NS, "sequoiadb", "Sequoiadb index configuration");

    public static final ConfigOption<String[]> SEQUOIADB_HOSTS = new ConfigOption<String[]>(SEQUOIADB_NS,"hosts",
        "Hosts of Sequoiadb cluster's coord info",
        ConfigOption.Type.MASKABLE, new String[] { "localhost" });

    public static final ConfigOption<Integer> SEQUOIADB_SERVICE = new ConfigOption<Integer>(SEQUOIADB_NS,"service",
        "service of Sequoiadb cluster's coord info",
        ConfigOption.Type.MASKABLE, Integer.class, SequoiadbStoreConfig.DEFAULTPORT, disallowEmpty(Integer.class));

    public static final ConfigOption<String> SEQUOIADB_USER = new ConfigOption<String>(SEQUOIADB_NS,"user",
        "User of Sequoiadb cluster's coord info",
        ConfigOption.Type.MASKABLE, String.class,"", disallowEmpty(String.class));

    public static final ConfigOption<String> SEQUOIADB_PASSWD = new ConfigOption<String>(SEQUOIADB_NS,"password",
        "Password of Sequoiadb cluster's coord info",
        ConfigOption.Type.MASKABLE, String.class, "", disallowEmpty(String.class));

    public static final ConfigOption<String> SEQUOIADB_SPACENAME = new ConfigOption<String>(SEQUOIADB_NS,"spacename",
        "janusgraph index sequoiadb collection space name",
        ConfigOption.Type.MASKABLE, String.class, "janusgraph_index", disallowEmpty(String.class));

    private String[] hostnames;
    private int service;
    private String dbUser = "";
    private String dbPasswd = "";
    private String csName = "";
    private final Map<String, DBCollection> stores = null;
    private SequoiadbDatasource ds = null;

    private static final IndexFeatures SEQUOIADB_FEATURES = new IndexFeatures.Builder()
        .supportsDocumentTTL()
        .setDefaultStringMapping(Mapping.TEXT)
        .supportedStringMappings(Mapping.TEXT, Mapping.STRING)
        .supportsCardinality(Cardinality.SINGLE)
        .supportsCardinality(Cardinality.LIST)
        .supportsCardinality(Cardinality.SET)
        .supportsCustomAnalyzer()
        .supportsGeoContains()
        .build();

    public SequoiadbIndex (final Configuration config) throws BackendException {

        hostnames = config.get(SEQUOIADB_HOSTS);
        service = config.get(SEQUOIADB_SERVICE);
        dbUser = config.get(SEQUOIADB_USER);
        dbPasswd = config.get(SEQUOIADB_PASSWD);

        csName = config.get(SEQUOIADB_SPACENAME);

        ds = SequoiadbHelper.initSequoiadbDs(hostnames,
            service,
            dbUser,
            dbPasswd);

    }

    @Override
    public void register(String store, String key, KeyInformation information, BaseTransaction tx) throws BackendException {
        Sequoiadb conn = null;
        try {
             conn = this.ds.getConnection();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        DBCollection CL = initCLHandler (conn, store);

        stores.put (store, CL);

    }

    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {

    }

    @Override
    public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {

    }

    @Override
    public Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        return null;
    }

    @Override
    public Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        return null;
    }

    @Override
    public Long totals(RawQuery query, KeyInformation.IndexRetriever information, BaseTransaction tx) throws BackendException {
        return null;
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return null;
    }

    @Override
    public void close() throws BackendException {

    }

    @Override
    public void clearStorage() throws BackendException {

    }

    @Override
    public boolean exists() throws BackendException {
        // 这个函数是用来判断 远端的 index 服务是否存在，并且可以工作的
        return false;
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        return false;
    }

    @Override
    public boolean supports(KeyInformation information) {
        return false;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        return null;
    }

    @Override
    public IndexFeatures getFeatures() {
        return SEQUOIADB_FEATURES;
    }

    private DBCollection initCLHandler (Sequoiadb conn, String clName) {
        CollectionSpace CS = null;
        DBCollection CL = null;
        if (!conn.isCollectionSpaceExist(csName)) { // create collection space
            try {
                Thread.sleep(10);
                if (!conn.isCollectionSpaceExist(csName)) {
                    conn.createCollectionSpace(csName);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        CS = conn.getCollectionSpace(csName);

        if (!CS.isCollectionExist(clName)) { // create collection
            try {
                Thread.sleep(50);
                if (!CS.isCollectionExist(clName)) {
                    CS.createCollection(clName);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        CL = CS.getCollection(clName);

        return CL;
    }
}
