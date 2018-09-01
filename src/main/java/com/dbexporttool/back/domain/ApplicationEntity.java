package com.dbexporttool.back.domain;

import javaslang.Tuple;
import javaslang.Tuple3;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Nick Barban.
 */

public class ApplicationEntity {

    private final List<Tuple3<String, Object, ? extends Class>> data;

    public ApplicationEntity(Map<String, Object> data) {
        this.data = data.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(entry -> Tuple.of(entry.getKey(), entry.getValue(), entry.getValue() == null ? Object.class : entry.getValue().getClass()))
                .collect(Collectors.toList());
    }

    public List<Tuple3<String, Object, ? extends Class>> getData() {
        return data;
    }
}
