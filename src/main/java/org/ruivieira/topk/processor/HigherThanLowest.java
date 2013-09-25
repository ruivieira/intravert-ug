package org.ruivieira.topk.processor;

import com.google.common.collect.Ordering;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.usergrid.vx.experimental.TypeHelper;
import org.usergrid.vx.server.operations.HandlerUtils;
import org.vertx.java.core.json.JsonObject;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Rui Vieira
 */
public class HigherThanLowest {

    private List<Map> queryResult = new ArrayList<Map>();

    public static List<Map> process(Map<Integer,Object> input, Map params) {
        System.out.println("[Called HigherThanLowest] Input: " + input);
        System.out.println("[Called HigherThanLowest] Params: " + params);
        final List<Map> result = new ArrayList<Map>();
        final Integer operation = (Integer) params.get("operation");

        final Long T = (long) (Integer) params.get("T");

        final int k = (Integer) params.get("k");

        final String keyspace = (String) params.get("keyspace");

        final String inverse = (String) params.get("inverseTableName");
        final String bucketName = (String) params.get("bucketName");
        final String bucketId = (String) params.get("bucketId");
        final String forward = (String) params.get("tableName");

        final String id = (String) params.get("objectName");
        final String score = (String) params.get("scoreName");

        System.out.println("[Called HigherThanLowest] Operation: " + operation);

        System.out.println("[Called HigherThanLowest] Operation values: " + input.get(operation.toString()));

        final List<Map> queryResult = (List<Map>) input.get(operation.toString());
        final List<Pair> pairs = new ArrayList<Pair>();

        for (Map m : queryResult) {
            final Map map = new HashMap();
            final Pair pair = Pair.create((String) m.get(id), (long) (Integer) m.get(score));
            pairs.add(pair);
        }

        final long lowestScore = Ordering.natural().greatestOf(pairs, k).get(k - 1).getScore();

        final long Ti = Math.max(T, lowestScore);

        System.out.println("[Called HigherThanLowest] Threshold:\t" + T);
        System.out.println("[Called HigherThanLowest] Lowest:\t" + lowestScore);

        ClientState clientState = new ClientState();
        try {
            clientState.setCQLVersion("3.0.0");
            clientState.setKeyspace(keyspace);
        } catch (InvalidRequestException e) {
            e.printStackTrace(); // TODO: Change
        }
        QueryState queryState = new QueryState(clientState);

        // TODO: Type checking of Peers
        final String queryCQL = String.format("SELECT %s, %s FROM %s WHERE %s = '%s' AND %s > %d",
                id, score, inverse, bucketName, bucketId, score, Ti);

        System.out.println("[Called HigherThanLowest] Query:\t" + queryCQL);

        ResultMessage rm = null;
        try {
            rm = QueryProcessor.process(queryCQL, ConsistencyLevel.ONE, queryState);
        } catch (RequestExecutionException | RequestValidationException e) {
            System.out.println(e.getMessage());
        }

        if (rm.kind == ResultMessage.Kind.ROWS) {
            ResultMessage.Rows cqlRows = (ResultMessage.Rows) rm;
            List<ColumnSpecification> columnSpecs = cqlRows.result.metadata.names;

            for (List<ByteBuffer> row : cqlRows.result.rows) {
                Map<String,Object> map = new HashMap<String,Object>();
                int i = 0;
                for (ByteBuffer bytes : row) {
                    ColumnSpecification specs = columnSpecs.get(i++);
                    map.put(specs.name.toString(), specs.type.compose(bytes));
                }
                result.add(map);
            }
        }

        return result;
    }

}
