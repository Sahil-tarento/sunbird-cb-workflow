package org.sunbird.workflow.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.sunbird.workflow.config.Constants;
import org.sunbird.workflow.models.Response;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

@Component
public class CassandraOperationImpl implements CassandraOperation {

    private Logger logger = LoggerFactory.getLogger(getClass().getName());
    protected CassandraConnectionManager connectionManager = CassandraConnectionMngrFactory.getInstance();

    @Override
    public List<Map<String, Object>> getRecordsByProperties(String keyspaceName, String tableName,
                                                            Map<String, Object> propertyMap, List<String> fields) {
        List<Map<String, Object>> response = new ArrayList<>();
        try (CqlSession session = connectionManager.getSession(keyspaceName)) {
            Select selectQuery = processQuery(keyspaceName, tableName, propertyMap, fields);
            ResultSet results = session.execute(selectQuery.build());
            response = CassandraUtil.createResponse(results);
        } catch (Exception e) {
            logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
        }
        return response;
    }

    @Override
    public int getCountByProperties(String keyspaceName, String tableName, Map<String, Object> propertyMap) {
        int count = 0;
        try (CqlSession session = connectionManager.getSession(keyspaceName)) {
            Select selectQuery = selectFrom(keyspaceName, tableName).countAll().where();
            propertyMap.forEach((key, value) -> selectQuery.whereColumn(key).isEqualTo(bindMarker()));
            PreparedStatement preparedStatement = session.prepare(selectQuery.build());
            BoundStatement boundStatement = preparedStatement.bind(propertyMap.values().toArray());
            ResultSet resultSet = session.execute(boundStatement);
            Row row = resultSet.one();
            if (row != null) {
                count = (int) row.getLong(0);
            }
        } catch (Exception e) {
            logger.error(Constants.EXCEPTION_MSG_FETCH + tableName + " : " + e.getMessage(), e);
        }
        return count;
    }

    private Select processQuery(String keyspaceName, String tableName, Map<String, Object> propertyMap, List<String> fields) {
        Select selectFrom;
        if (CollectionUtils.isNotEmpty(fields)) {
            selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName).columns(fields.toArray(new String[0]));
        } else {
            selectFrom = QueryBuilder.selectFrom(keyspaceName, tableName).all();
        }

        Select selectQuery = selectFrom.all();
        if (MapUtils.isNotEmpty(propertyMap)) {
            for (Entry<String, Object> entry : propertyMap.entrySet()) {
                if (entry.getValue() instanceof List) {
                    List<?> list = (List<?>) entry.getValue();
                    if (list != null) {
                        selectQuery = selectQuery.whereColumn(entry.getKey()).in((BindMarker) list);
                    }
                } else {
                    selectQuery = selectQuery.whereColumn(entry.getKey()).isEqualTo(QueryBuilder.literal(entry.getValue()));
                }
            }
            selectQuery = selectQuery.allowFiltering();
        }

        return selectQuery;
    }

    public Response insertRecord(String keyspaceName, String tableName, Map<String, Object> request) {
        Response response = new Response();
        try (CqlSession session = connectionManager.getSession(keyspaceName)) {
            String query = CassandraUtil.getPreparedStatement(keyspaceName, tableName, request);
            PreparedStatement statement = session.prepare(query);
            BoundStatement boundStatement = statement.bind(request.values().toArray());
            session.execute(boundStatement);
            response.put("STATUS", "SUCCESS");
        } catch (Exception e) {
            String errorMessage = String.format("Exception occurred while inserting record to %s %s", tableName, e.getMessage());
            logger.error(errorMessage, e);
            response.put("STATUS", "FAILED");
        }
        return response;
    }

    public Map<String, Object> updateRecord(String keyspaceName, String tableName, Map<String, Object> updateAttributes,
                                            Map<String, Object> compositeKey) {
        Map<String, Object> response = new HashMap<>();
        try (CqlSession session = connectionManager.getSession(keyspaceName)) {
            UpdateStart updateStart = QueryBuilder.update(keyspaceName, tableName);
            UpdateWithAssignments updateWithAssignments = updateStart.set(updateAttributes.entrySet().stream()
                    .map(entry -> Assignment.setColumn(entry.getKey(), QueryBuilder.literal(entry.getValue())))
                    .toArray(Assignment[]::new));
            Update update = updateWithAssignments.where(compositeKey.entrySet().stream()
                    .map(entry -> Relation.column(entry.getKey()).isEqualTo(QueryBuilder.literal(entry.getValue())))
                    .toArray(Relation[]::new));
            SimpleStatement statement = update.build();
            session.execute(statement);
            response.put(Constants.RESPONSE, Constants.SUCCESS);
        } catch (Exception e) {
            String errMsg = String.format("Exception occurred while updating record to %s: %s", tableName, e.getMessage());
            logger.error(errMsg, e);
            response.put(Constants.RESPONSE, Constants.FAILED);
            response.put(Constants.ERROR_MESSAGE, errMsg);
            throw e;
        }
        return response;
    }
}