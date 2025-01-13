package org.sunbird.workflow.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.sunbird.workflow.config.Constants;
import org.sunbird.workflow.exception.ProjectCommonException;
import org.sunbird.workflow.exception.ResponseCode;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class CassandraConnectionManagerImpl implements CassandraConnectionManager {

	private static final Map<String, CqlSession> cassandraSessionMap = new ConcurrentHashMap<>(2);
	private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionManagerImpl.class);
	private static CqlSession session;

	@Override
	public CqlSession getSession(String keyspaceName) {
		// Check if session for keyspace already exists
		CqlSession currentSession = cassandraSessionMap.get(keyspaceName);
		if (currentSession != null) {
			return currentSession;
		} else {
			// Create new session scoped to keyspace using the USE command
			session.execute("USE " + keyspaceName);
			cassandraSessionMap.put(keyspaceName, session);
			return session;
		}
	}

	public CassandraConnectionManagerImpl() {
		// Initialize the connection and register shutdown hook
		registerShutDownHook();
		createCassandraConnection();
	}

	private void createCassandraConnection() {
		try {
			// Load the properties required for connection
			PropertiesCache cache = PropertiesCache.getInstance();
			String cassandraHost = cache.getProperty(Constants.CASSANDRA_CONFIG_HOST);
			if (StringUtils.isBlank(cassandraHost)) {
				throw new ProjectCommonException(
						ResponseCode.internalError.getErrorCode(),
						"Cassandra host is not configured",
						ResponseCode.SERVER_ERROR.getResponseCode());
			}

			List<String> hosts = Arrays.asList(cassandraHost.split(","));
			List<InetSocketAddress> contactPoints = hosts.stream()
					.map(host -> new InetSocketAddress(host.trim(), 9042)) // Assuming default port 9042
					.collect(Collectors.toList());

			DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
					.withStringList(DefaultDriverOption.CONTACT_POINTS, hosts)
					.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "datacenter1")
					// Local host connection pooling
					.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE,
							Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_LOCAL)))
					// Remote host connection pooling
					.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE,
							Integer.parseInt(cache.getProperty(Constants.CORE_CONNECTIONS_PER_HOST_FOR_REMOTE)))
					// Heartbeat and timeout settings
					.withInt(DefaultDriverOption.HEARTBEAT_INTERVAL,
							Integer.parseInt(cache.getProperty(Constants.HEARTBEAT_INTERVAL)))
					.withInt(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, 10000)
					.withInt(DefaultDriverOption.REQUEST_TIMEOUT, 10000)
					.withString(DefaultDriverOption.PROTOCOL_VERSION, ProtocolVersion.V4.toString())  // Protocol version for Cassandra
					.withClass(DefaultDriverOption.RETRY_POLICY_CLASS, DefaultRetryPolicy.class)  // Retry policy
					.build();

			// Create the CqlSession with the configuration loader
			session = CqlSession.builder()
					.addContactPoints(contactPoints)
					.withLocalDatacenter("datacenter1")
					.withConfigLoader(loader)
					.build();

			// Get metadata and log cluster information
			final Metadata metadata = session.getMetadata();
			logger.info(String.format("Connected to cluster: %s", metadata.getClusterName()));

			// Log nodes in the cluster
			for (Node host : metadata.getNodes().values()) {
				logger.info(String.format("Datacenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getEndPoint(), host.getRack()));
			}
		} catch (Exception e) {
			logger.error("Error while creating Cassandra connection", e);
			throw new ProjectCommonException(
					ResponseCode.internalError.getErrorCode(),
					e.getMessage(),
					ResponseCode.SERVER_ERROR.getResponseCode());
		}
	}

	@Override
	public List<String> getTableList(String keyspaceName) {
		try {
			// Fetch the metadata for the keyspace and list tables
			Metadata metadata = session.getMetadata();
			if (metadata.getKeyspace(keyspaceName).isPresent()) {
				// Convert the Map<CqlIdentifier, TableMetadata> to a List<String> with table names
				Map<CqlIdentifier, TableMetadata> tables = metadata.getKeyspace(keyspaceName).get().getTables();
				return tables.keySet().stream()
						.map(CqlIdentifier::toString) // Convert CqlIdentifier to string (table names)
						.collect(Collectors.toList());
			} else {
				throw new ProjectCommonException(
						ResponseCode.internalError.getErrorCode(),
						"Keyspace not found: " + keyspaceName,
						ResponseCode.SERVER_ERROR.getResponseCode());
			}
		} catch (Exception e) {
			logger.error("Error fetching tables for keyspace: " + keyspaceName, e);
			throw new ProjectCommonException(
					ResponseCode.internalError.getErrorCode(),
					e.getMessage(),
					ResponseCode.SERVER_ERROR.getResponseCode());
		}
	}

	// Register shutdown hook to clean up resources when JVM shuts down
	public static void registerShutDownHook() {
		Runtime runtime = Runtime.getRuntime();
		runtime.addShutdownHook(new ResourceCleanUp());
		logger.info("Cassandra ShutDownHook registered.");
	}

	// Clean up resources when JVM terminates
	static class ResourceCleanUp extends Thread {
		@Override
		public void run() {
			try {
				logger.info("Started resource cleanup for Cassandra.");
				for (Map.Entry<String, CqlSession> entry : cassandraSessionMap.entrySet()) {
					entry.getValue().close();
				}
				if (session != null) {
					session.close();
				}
				logger.info("Completed resource cleanup for Cassandra.");
			} catch (Exception ex) {
				logger.error("Error during resource cleanup", ex);
			}
		}
	}
}