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
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.IFilter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Rui Vieira
 */
public class CLF {

    private List<Map> queryResult = new ArrayList<Map>();

    public static final double DEFAULT_MAX_FALSE_POSITIVE_RATE = 0.004;

    public static List<Map> process(Map<Integer,Object> input, Map params) {
//        System.out.println("[Called HigherThanLowest] Input: " + input);
//        System.out.println("[Called HigherThanLowest] Params: " + params);
        final List<Map> result = new ArrayList<Map>();
        final Integer operation = (Integer) params.get("operation");

//        final int k = (Integer) params.get("k");
        final long mink = new Long((Integer) params.get("mink"));

//        final String keyspace = (String) params.get("keyspace");
//
//        final String inverse = (String) params.get("inverseTableName");
//        final String bucketName = (String) params.get("bucketName");
//        final String bucketId = (String) params.get("bucketId");
//        final String forward = (String) params.get("tableName");

        final String id = (String) params.get("objectName");
//        final String score = (String) params.get("scoreName");

//        System.out.println("[Called HigherThanLowest] Operation: " + operation);
//
//        System.out.println("[Called HigherThanLowest] Operation values: " + input.get(operation.toString()));

        final List<Map> queryResult = (List<Map>) input.get(operation.toString());

//        System.out.println("[Called HigherThanLowest] k: " + k + " query results: " + queryResult.size());

        final IFilter filter = FilterFactory.getFilter(1000, 1, false);

        for (Map m : queryResult) {
            final String _id = (String) m.get(id);
            final ByteBuffer bid = ByteBuffer.wrap(_id.getBytes());
            filter.add(bid);
        }

        final Map map = new HashMap();

        final ByteBuffer serialisedFilter = serialize(filter);

        map.put("filter", Hex.bytesToHex(serialisedFilter.array()));

        result.add(map);

        return result;
    }

    public static ByteBuffer serialize(final IFilter filter) {

        final ByteArrayOutputStream b = new ByteArrayOutputStream();

        final DataOutput output = new DataOutputStream(b);

        try {

            FilterFactory.serialize(filter, output);

        } catch (IOException e) {

            System.out.println(e.getMessage());

        }

        return ByteBuffer.wrap(b.toByteArray());
    }


}
