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

import com.sequoiadb.base.ConfigOptions;
import com.sequoiadb.datasource.ConnectStrategy;
import com.sequoiadb.datasource.DatasourceOptions;
import com.sequoiadb.datasource.SequoiadbDatasource;

import java.util.ArrayList;
import java.util.List;

public class SequoiadbHelper {

    public static SequoiadbDatasource initSequoiadbDs (String[] hosts,
                                                 int port,
                                                 String user,
                                                 String password) {
        SequoiadbDatasource ds = null;

        List<String> addrs = new ArrayList<String>();

        for (String h : hosts) {
            System.out.println ("hostname = " + h);
            addrs.add (h + ":" + port);
        }

        ConfigOptions configOpt = new ConfigOptions();
        DatasourceOptions dsOpt = new DatasourceOptions();
        configOpt.setConnectTimeout(500);
        configOpt.setMaxAutoConnectRetryTime(0);
        dsOpt.setMaxCount(3000);
        dsOpt.setDeltaIncCount(20);
        dsOpt.setMaxIdleCount(20);
        dsOpt.setKeepAliveTimeout(0);
        dsOpt.setCheckInterval(60000);
        dsOpt.setSyncCoordInterval(0);

        dsOpt.setValidateConnection(false);
        dsOpt.setConnectStrategy(ConnectStrategy.BALANCE);
        ds = new SequoiadbDatasource(addrs, user, password, configOpt, dsOpt);

        return ds;
    }
}
