package org.ruivieira.topk.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Rui Vieira
 */
public class HigherThanLowest {

    public static List<Map> process(Map<Integer,Object> input, Map params) {
        System.out.println("[Called HigherThanLowest] Input: " + input);
        System.out.println("[Called HigherThanLowest] Params: " + params);
        final List<Map> result = new ArrayList<>();
        final Integer operation = (Integer) params.get("operation");
        System.out.println("[Called HigherThanLowest] Operation: " + operation);

        System.out.println("[Called HigherThanLowest] Operation values: " + input.get(operation.toString()));
        for (Map m : (List<Map>) input.get(operation.toString())) {
            final Map map = new HashMap();
            final String id = (String) params.get("objectName");
            final String score = (String) params.get("scoreName");
            map.put(id, m.get(id));
            map.put(score, 11);
            result.add(map);
        }
        return result;
    }
}
