package org.sunbird.workflow.utils;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.List;

/**
 * Interface for cassandra connection manager , implementation would be Standalone and Embedde
 * cassandra connection manager 
 * @author fathima
 */
public interface CassandraConnectionManager {

	/**
	   * Method to get the cassandra session oject on basis of keyspace name provided .
	   * @param keyspaceName
	   * @return Session
	   */
	  CqlSession getSession(String keyspaceName);

	List<String> getTableList(String keyspacename);
}
