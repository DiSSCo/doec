package eu.dissco.doec.utils;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class FlatMapUtils {


    /******************/
    /* PUBLIC METHODS */
    /******************/

    /**
     * Flat the Map object received as parameter
     * @param map Map to be flatter
     * @return Map flatten
     */
    public static Map<String, Object> flatten(Map<String, Object> map) {
        return map.entrySet().stream()
                .flatMap(FlatMapUtils::flatten)
                .collect(LinkedHashMap::new, (m, e) -> m.put("/" + e.getKey(), e.getValue()), LinkedHashMap::putAll);
    }


    /*******************/
    /* PRIVATE METHODS */
    /*******************/

    /**
     * Flat a map entry
     * @param entry Map entry to be flatten
     * @return Stream with the map entry flatten
     */
    private static Stream<Map.Entry<String, Object>> flatten(Map.Entry<String, Object> entry) {
        if (entry == null) {
            return Stream.empty();
        }

        if (entry.getValue() instanceof Map<?, ?>) {
            return ((Map<?, ?>) entry.getValue()).entrySet().stream()
                    .flatMap(e -> flatten(new AbstractMap.SimpleEntry<>(entry.getKey() + "/" + e.getKey(), e.getValue())));
        }

        if (entry.getValue() instanceof List<?>) {
            List<?> list = (List<?>) entry.getValue();
            return IntStream.range(0, list.size())
                    .mapToObj(i -> new AbstractMap.SimpleEntry<String, Object>(entry.getKey() + "/" + i, list.get(i)))
                    .flatMap(FlatMapUtils::flatten);
        }

        return Stream.of(entry);
    }
}