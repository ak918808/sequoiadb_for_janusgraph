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

import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.datasource.SequoiadbDatasource;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;

import java.util.HashMap;
import java.util.Map;

public class SequoiadbTx extends AbstractStoreTransaction {

    private Map<String, Sequoiadb> connMap = new HashMap<String, Sequoiadb>();
//    private SequoiadbDatasource ds = null;

    public SequoiadbTx(BaseTransactionConfig config) throws BackendException {
        super(config);
//        this.ds = ds;
//        try {
//            initConn();
//        } catch (InterruptedException e) {
//            throw new BackendException("get Sequoiadb conn fail, error = " + e.getMessage());
//        }
    }



    @Override
    public void rollback() throws BackendException {
        super.rollback();
        for (Map.Entry<String, Sequoiadb> e : connMap.entrySet()) {
            Sequoiadb conn = e.getValue();
            conn.rollback();
        }
    }

    @Override
    public void commit() throws BackendException {
        super.commit();
        for (Map.Entry<String, Sequoiadb> e : connMap.entrySet()) {
            Sequoiadb conn = e.getValue();
            conn.commit();
        }
    }

    public synchronized Sequoiadb getConn (String name) {
        if (connMap.containsKey(name)) {
            return connMap.get (name);
        }
        else return null;
    }

    public synchronized void setConn (Sequoiadb conn, String name) {
        if (!connMap.containsKey(name)) {
            connMap.put (name, conn);
        }
    }
}
