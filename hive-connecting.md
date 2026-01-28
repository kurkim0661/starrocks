Code-Level Architecture for Hive Metastore Connection

1. Entry Point: HiveConnector (HiveConnector.java:41-50)

When you create a Hive catalog, StarRocks instantiates HiveConnector:

public HiveConnector(ConnectorContext context) {
this.properties = context.getProperties();
this.catalogName = context.getCatalogName();
CloudConfiguration cloudConfiguration =
CloudConfigurationFactory.buildCloudConfigurationForStorage(properties);
HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(cloudConfiguration);
this.internalMgr = new HiveConnectorInternalMgr(catalogName, properties,
hdfsEnvironment);
this.metadataFactory = createMetadataFactory(hdfsEnvironment);
onCreate();
}

2. Metastore Creation: HiveConnectorInternalMgr
   (HiveConnectorInternalMgr.java:115-138)

The internal manager creates the metastore client with caching layers:

public IHiveMetastore createHiveMetastore() {
// Create base Hive meta client
HiveMetaClient metaClient = HiveMetaClient.createHiveMetaClient(hdfsEnvironment,
properties);
IHiveMetastore hiveMetastore = new HiveMetastore(metaClient, catalogName,
metastoreType);

      if (!enableMetastoreCache) {
          baseHiveMetastore = hiveMetastore;
      } else {
          // Wrap with caching layer
          refreshHiveMetastoreExecutor = Executors.newCachedThreadPool(...);
          refreshHiveExternalTableExecutor = Executors.newCachedThreadPool(...);
          baseHiveMetastore = CachingHiveMetastore.createCatalogLevelInstance(
              hiveMetastore,
              new ReentrantExecutor(refreshHiveMetastoreExecutor, ...),
              new ReentrantExecutor(refreshHiveExternalTableExecutor, ...),
              hmsConf.getCacheTtlSec(),
              hmsConf.getCacheRefreshIntervalSec(),
              hmsConf.getCacheMaxNum(),
              hmsConf.enableListNamesCache()
          );
      }
      return baseHiveMetastore;
}

3. Connection Pool: HiveMetaClient (HiveMetaClient.java:76-160)

HiveMetaClient manages a connection pool of recyclable Thrift clients:

Key Components:

a) Connection Pool (lines 68-69):
private final LinkedList<RecyclableClient> clientPool = new LinkedList<>();
private final Object clientPoolLock = new Object();

b) RecyclableClient Inner Class (lines 96-129):
public class RecyclableClient {
private final IMetaStoreClient hiveClient;

      private RecyclableClient(HiveConf conf) throws MetaException {
          if (DLF_HIVE_METASTORE.equalsIgnoreCase(conf.get(HIVE_METASTORE_TYPE))) {
              hiveClient = RetryingMetaStoreClient.getProxy(conf, DUMMY_HOOK_LOADER,
                  DLFProxyMetaStoreClient.class.getName());
          } else if
(GLUE_HIVE_METASTORE.equalsIgnoreCase(conf.get(HIVE_METASTORE_TYPE))) {
hiveClient = RetryingMetaStoreClient.getProxy(conf, DUMMY_HOOK_LOADER,
AWSCatalogMetastoreClient.class.getName());
} else {
// Standard Hive Metastore
hiveClient = RetryingMetaStoreClient.getProxy(conf, DUMMY_HOOK_LOADER,
HiveMetaStoreClient.class.getName());
}
}

      // Recycle or close client after use
      public void finish() {
          synchronized (clientPoolLock) {
              if (clientPool.size() >= maxPoolSize) {
                  close();
              } else {
                  clientPool.offer(this);  // Return to pool
              }
          }
      }
}

c) Get Client from Pool (lines 139-160):
private RecyclableClient getClient() throws MetaException {
synchronized (clientPoolLock) {
RecyclableClient client = clientPool.poll();
if (client == null) {
// Create new client if pool is empty
return new RecyclableClient(conf);
} else {
return client;  // Reuse existing client
}
}
}

d) RPC Call Handler (lines 162-191):
public <T> T callRPC(String methodName, String messageIfError, Object... args) {
RecyclableClient client = null;
try {
client = getClient();
Method method = client.hiveClient.getClass().getDeclaredMethod(methodName,
argClasses);
return (T) method.invoke(client.hiveClient, args);
} catch (Throwable e) {
throw new StarRocksConnectorException(messageIfError + ", msg: " +
e.getMessage(), e);
} finally {
if (connectionException != null) {
client.close();  // Close on error
} else if (client != null) {
client.finish();  // Return to pool
}
}
}

4. Thrift Connection: HiveMetaStoreClient (HiveMetaStoreClient.java:443-599)

This is where the actual Thrift connection is established:

Connection Process:

a) Parse Metastore URIs (lines 310-351):
private void resolveUris() throws MetaException {
String[] metastoreUrisString = MetastoreConf.getVar(conf,
ConfVars.THRIFT_URIS).split(",");
List<URI> metastoreURIArray = new ArrayList<>();
for (String s : metastoreUrisString) {
URI tmpUri = new URI(s);  // Parse thrift://host:port
metastoreURIArray.add(tmpUri);
}
metastoreUris = metastoreURIArray.toArray(new URI[0]);
}

b) Establish Thrift Connection (lines 451-596):
private void openInternal() throws MetaException {
boolean useSSL = MetastoreConf.getBoolVar(conf, ConfVars.USE_SSL);
boolean useSasl = MetastoreConf.getBoolVar(conf, ConfVars.USE_THRIFT_SASL);
boolean useFramedTransport = MetastoreConf.getBoolVar(conf,
ConfVars.USE_THRIFT_FRAMED_TRANSPORT);
boolean useCompactProtocol = MetastoreConf.getBoolVar(conf,
ConfVars.USE_THRIFT_COMPACT_PROTOCOL);
int clientSocketTimeout = (int) MetastoreConf.getTimeVar(conf,
ConfVars.CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

      // Retry logic
      for (int attempt = 0; !isConnected && attempt < retries; ++attempt) {
          for (URI store : metastoreUris) {
              try {
                  // 1. Create transport layer
                  if (useSSL) {
                      transport = SecurityUtils.getSSLSocket(store.getHost(),
store.getPort(), clientSocketTimeout, ...);
} else {
transport = new TSocket(store.getHost(), store.getPort(),
clientSocketTimeout);
}

                  // 2. Add SASL security layer (if enabled)
                  if (useSasl) {
                      HadoopThriftAuthBridge.Client authBridge =
HadoopThriftAuthBridge.getBridge().createClient();
if (tokenStrForm != null) {
// DIGEST authentication with delegation tokens
transport = authBridge.createClientTransport(null,
store.getHost(),
"DIGEST", tokenStrForm, transport, ...);
} else {
// Kerberos authentication
transport = authBridge.createClientTransport(principalConfig,
store.getHost(),
"KERBEROS", null, transport, ...);
}
} else if (useFramedTransport) {
transport = new TFramedTransport(transport);
}

                  // 3. Create protocol layer
                  final TProtocol protocol;
                  if (useCompactProtocol) {
                      protocol = new TCompactProtocol(transport);
                  } else {
                      protocol = new TBinaryProtocol(transport);
                  }

                  // 4. Create Thrift client
                  client = new ThriftHiveMetastore.Client(protocol);

                  // 5. Open connection
                  if (!transport.isOpen()) {
                      transport.open();
                      LOG.info("Opened a connection to metastore, current connections: "
+ connCount.incrementAndGet());
  }
  isConnected = true;

               // 6. Set UGI (User Group Information) for non-secure mode
               if (isConnected && !useSasl) {
                   UserGroupInformation ugi =
HadoopExt.getInstance().getHMSUGI(conf);
client.set_ugi(ugi.getUserName(),
Arrays.asList(ugi.getGroupNames()));
}
} catch (TTransportException e) {
LOG.error("Unable to connect to metastore with URI " + store);
}
}
// Wait before retry
if (!isConnected && retryDelaySeconds > 0) {
Thread.sleep(retryDelaySeconds * 1000);
}
}
}

5. Caching Layer: CachingHiveMetastore (CachingHiveMetastore.java)

Wraps the base metastore with Guava/Caffeine caches:

protected CachingHiveMetastore(...) {
// Database names cache
databaseNamesCache = newCacheBuilder(...).build();

      // Table names cache
      tableNamesCache = newCacheBuilder(...).build();

      // Partition keys cache (e.g., year=2022/month=10)
      partitionKeysCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec,
maxSize)
.build(asyncReloading(CacheLoader.from(this::loadPartitionKeys), executor));

      // Partition metadata cache
      partitionCache = newCacheBuilder(expireAfterWriteSec, refreshIntervalSec, maxSize)
          .build(asyncReloading(new CacheLoader<HivePartitionName, Partition>() {...},
partitionExecutor));

      // Statistics caches
      tableStatsCache = newCacheBuilder(...).build();
      partitionStatsCache = builder.buildAsync(new PartitionStatisticsLoader(this));
}

Architecture Diagram

User Query
↓
HiveConnector (Entry point)
↓
HiveConnectorInternalMgr (Factory)
↓
CachingHiveMetastore (Cache layer) ← Caffeine/Guava caches
↓
HiveMetastore (Base implementation)
↓
HiveMetaClient (Connection pool manager)
↓
RecyclableClient (Pooled Thrift client)
↓
RetryingMetaStoreClient (Retry wrapper)
↓
HiveMetaStoreClient (Actual Thrift client)
↓
TSocket/TTransport (Thrift transport layer)
↓
Hive Metastore Server (thrift://host:9083)

Key Configuration Parameters

From the code:
- hive.metastore.uris: Metastore URIs (line HiveMetaClient.java:86)
- hive.metastore.timeout: Socket timeout (line 88)
- hive.metastore.connection.pool.size: Max pool size (line 89)
- enable_metastore_cache: Enable caching (HiveConnectorInternalMgr.java:70)
- enable_remote_file_cache: Enable file cache (line 73)

This architecture provides connection pooling, automatic retries, caching, and HA
support for efficient Hive metastore access!

---
Credential Sources

1. User-Provided Properties (Primary Source)

When you create a Hive catalog, credentials come from the PROPERTIES you specify in the CREATE CATALOG statement. These properties are passed through the following flow:

HiveConnector.java:81-94 (createHiveMetaClient)
↓
HiveConf conf = new HiveConf();
conf.addResource(env.getConfiguration());  // Load Hadoop configs
properties.forEach(conf::set);              // User properties override

2. Configuration File Paths

Users can point to Hadoop/Hive configuration files using:

- hadoop.config.resources - Path to config files (hive-site.xml, core-site.xml, hdfs-site.xml)

Example from CloudConfiguration.java:76-79:
configResources = properties.getOrDefault(HadoopExt.HADOOP_CONFIG_RESOURCES, "");

3. Kerberos Credentials

From CloudConfigurationConstants.java:130-139, StarRocks accepts:

- hadoop.kerberos.principal - Kerberos principal name
- hadoop.kerberos.keytab - Path to keytab file
- hadoop.kerberos.keytab_content - Base64-encoded keytab content
- hadoop.security.kerberos.ticket.cache.path - Ticket cache path

These are extracted in HDFSCloudConfigurationProvider.java:68-76:
HDFSCloudCredential hdfsCloudCredential = new HDFSCloudCredential(
getOrDefault(properties, HDFS_AUTHENTICATION),
getOrDefault(properties, HDFS_USERNAME, HDFS_USERNAME_DEPRECATED),
getOrDefault(properties, HDFS_PASSWORD, HDFS_PASSWORD_DEPRECATED),
getOrDefault(properties, HDFS_KERBEROS_PRINCIPAL, HDFS_KERBEROS_PRINCIPAL_DEPRECATED),
getOrDefault(properties, HADOOP_KERBEROS_KEYTAB, HDFS_KERBEROS_KEYTAB_DEPRECATED),
getOrDefault(properties, HADOOP_KERBEROS_KEYTAB_CONTENT, HDFS_KERBEROS_KEYTAB_CONTENT_DEPRECATED),
prop
);

4. Default Hadoop Configuration Loading

When new Configuration() is called in HdfsEnvironment.java:32, Hadoop automatically loads configuration files from:
- Classpath (if hive-site.xml, core-site.xml, hdfs-site.xml are present)
- Default Hadoop locations

5. Simple Authentication

For non-Kerberos setups from CloudConfigurationConstants.java:127-129:
- hadoop.username - Hadoop username
- hdfs.password - HDFS password (deprecated)
- hdfs.authentication = "simple" - Authentication mode

Where Credentials Are NOT Loaded

StarRocks does NOT automatically dig through the filesystem to find Hive configuration files. You must explicitly provide either:
1. Configuration via properties in CREATE CATALOG
2. Path to config files via hadoop.config.resources
3. Individual credential properties

This is different from native Hive which uses HIVE_CONF_DIR environment variable.

