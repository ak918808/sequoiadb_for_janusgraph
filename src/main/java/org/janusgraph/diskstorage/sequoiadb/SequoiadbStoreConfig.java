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

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import org.janusgraph.diskstorage.configuration.Configuration;

import static org.janusgraph.diskstorage.Backend.*;
import static org.janusgraph.diskstorage.Backend.SYSTEM_TX_LOG_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_STORE_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;

public class SequoiadbStoreConfig {
    public static String SEQUOIADB_CONF_CSNAME = "cs.name";
//    public static String SEQUOIADB_CONF_CLNAME = "cl.name";
//    public static String SEQUOIADB_CONF_HOSTS = "storage.hostname";
//    public static String SEQUOIADB_CONF_USER = "storage.user";
//    public static String SEQUOIADB_CONF_PASSWORD = "storage.password";
    public static final int DEFAULTPORT = 11810;

    public static BiMap<String, String> createShortCfMap(Configuration config) {
        return ImmutableBiMap.<String, String>builder()
            .put(INDEXSTORE_NAME, "g")
            .put(INDEXSTORE_NAME + LOCK_STORE_SUFFIX, "h")
            .put(config.get(IDS_STORE_NAME), "i")
            .put(EDGESTORE_NAME, "e")
            .put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, "f")
            .put(SYSTEM_PROPERTIES_STORE_NAME, "s")
            .put(SYSTEM_PROPERTIES_STORE_NAME + LOCK_STORE_SUFFIX, "t")
            .put(SYSTEM_MGMT_LOG_NAME, "m")
            .put(SYSTEM_TX_LOG_NAME, "l")
            .build();
    }
}
