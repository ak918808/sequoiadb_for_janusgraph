# 背景
本项目主要是为 JanusGraph 图计算数据库，增加 SequoiaDB 数据库作为它的一个存储源。
本身 JanusGraph 图计算数据库，就能够支持 HBase 和 Cassandra 作为它的数据存储源。但是由于工作关系，项目上已经使用了 SequoiaDB 数据库作为项目平台的主要 DB，所以作者就想着能不能为 JanusGraph 插上 SequoiaDB 的翅膀，使得项目的底层存储更加的统一。

目前，这个项目已经实现了 JanusGraph 从 SequoiaDB 数据库中存/取数据。虽然demo 比较简单，也还存在着不少的坑，但是也是一个良好的开始。

未来呢，作者还计划让 JanusGraph 的索引模块也使用 SequoiaDB 数据库来进行存储，毕竟 JSON 这种格式，SequoiaDB 也是比较擅长的。大家期待吧。

# 编译方式
本项目 SequoiaDB for JanusGraph plugin 插件基于 JanusGraph 0.4.0 版本开发和验证。

## JanusGraph core 模块
由于要给JanusGraph 增加新的存储引擎，所以需要对JanusGraph core 模块填加 SequoiaDB 的接口说明。

用户直接修改 ${JANUSGRAPH_SOURCE_HOME}/janusgraph-core/src/main/java/org/janusgraph/diskstorage/StandardStoreManager.java 文件，在 enum 部分增加SEQUOIADB 内容

```java
BDB_JE("org.janusgraph.diskstorage.berkeleyje.BerkeleyJEStoreManager", "berkeleyje"),
CASSANDRA_THRIFT("org.janusgraph.diskstorage.cassandra.thrift.CassandraThriftStoreManager", "cassandrathrift"),
CASSANDRA_ASTYANAX("org.janusgraph.diskstorage.cassandra.astyanax.AstyanaxStoreManager", ImmutableList.of("cassandra", "astyanax")),
CASSANDRA_EMBEDDED("org.janusgraph.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager", "embeddedcassandra"),
CQL("org.janusgraph.diskstorage.cql.CQLStoreManager", "cql"),
HBASE("org.janusgraph.diskstorage.hbase.HBaseStoreManager", "hbase"),
IN_MEMORY("org.janusgraph.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager", "inmemory"),
SEQUOIADB("org.janusgraph.diskstorage.sequoiadb.SequoiadbStoreManager", "sequoiadb");
```

> NOTE:
> 由于 janusgraph-core 的模块需要更新代码，当然也需要重新编译，并且将新的 janusgraph-cord 的jar 包替换原来 JanusGraph-binary 的jar 包。

# JanusGraph Maven 
为了能够让JanusGraph 在编译整个工程时，能够直接触发 SequoiaDB for JanusGraph plugin 插件的编译，用户可以修改 ${JANUSGRAPH_SOURCE_HOME}/pom.xml 文件，在里面添加 SequoiaDB 的内容

修改的第一部分
```xml
<properties>
        <titan.compatible-versions>1.0.0,1.1.0-SNAPSHOT</titan.compatible-versions>
        <tinkerpop.version>3.4.1</tinkerpop.version>
        <junit-platform.version>1.4.0</junit-platform.version>
        <junit.version>5.4.0</junit.version>
        <mrunit.version>1.1.0</mrunit.version>
        <mockito.version>2.23.0</mockito.version>
        <cassandra.version>2.2.13</cassandra.version>
        <jamm.version>0.3.0</jamm.version>
        <metrics.version>3.2.2</metrics.version>
        <sesame.version>2.7.10</sesame.version>
        <slf4j.version>1.7.12</slf4j.version>
        <httpcomponents.version>4.4.1</httpcomponents.version>
        <hadoop2.version>2.7.7</hadoop2.version>
        <hbase1.version>1.4.10</hbase1.version>
        <hbase2.version>2.1.5</hbase2.version>
        <hbase.server.version>1.4.10</hbase.server.version>
        <sequoiadb.version>3.2.1</sequoiadb.version>
        ...
```

修改的第二部分
```xml
<modules>
        <module>janusgraph-codepipelines-ci</module>
        <module>janusgraph-core</module>
        <module>janusgraph-server</module>
        <module>janusgraph-test</module>
        <module>janusgraph-berkeleyje</module>
        <module>janusgraph-cql</module>
        <module>janusgraph-cassandra</module>
        <module>janusgraph-hadoop-parent</module>
        <module>janusgraph-hbase-parent</module>
        <module>janusgraph-bigtable</module>
        <module>janusgraph-es</module>
        <module>janusgraph-lucene</module>
        <module>janusgraph-all</module>
        <module>janusgraph-dist</module>
        <module>janusgraph-doc</module>
        <module>janusgraph-solr</module>
        <module>janusgraph-examples</module>
        <module>janusgraph-sequoiadb</module>
    </modules>
```

## JanusGraph 源码编译方式

```bash
mvn -Dlicense.skip=true -DskipTests=true clean install
```

然后将 janusgraph-sequoiadb 编译的 janusgraph-sequoiadb-0.4.0.jar 和 SequoiaDB 的API 驱动jar 包保存至 ${JANUSGRAPH_BINARY_HOME}/lib 目录中。

## JanusGraph 对接 SequoiaDB

为 JanusGraph 数据库配置 SequoiaDB 的连接信息，*新增*一个 conf 配置文件，${JANUSGRAPH_BINARY_HOME}/conf/janusgraph-sequoiadb.properties，增加内容如下

```
gremlin.graph=org.janusgraph.core.JanusGraphFactory
storage.backend=sequoiadb
storage.hostname=10.211.55.7
storage.port=11810
#storage.username=sdbadmin
#storage.password=sdbadmin
storage.meta.visibility = true
cache.db-cache = false
cache.db-cache-clean-wait = 20
cache.db-cache-time = 180000
cache.db-cache-size = 0.5
```

比较重要的信息，就是为 JanusGraph 配置 SequoiaDB 的 Coord 节点的连接信息
* storage.hostname，coord 节点的 IP 地址，或者是 hostname
* storage.port，coord 节点的端口号
* storage.username，如果 SequoiaDB 配置了鉴权，那样就需要配置鉴权的用户名
* storage.password，如果 SequoiaDB 配置了鉴权，那样就需要配置鉴权的密码


## 验证测试

进入 JanusGraph 控制台
```
bin/gremlin.sh
```

加载 SequoiaDB 的配置信息
```
graph = JanusGraphFactory.open('conf/janusgraph-sequoiadb.properties');
```

在 JanusGraph 中向 SequoiaDB 写入一条demo 数据，然后查询出来
```
graph.addVertex("name", "aaa", "num", 123)
g = graph.traversal()
g.V().values('name')
```

退出 JanusGraph 控制台
```
:q
```
