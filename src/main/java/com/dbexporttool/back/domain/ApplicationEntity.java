package com.dbexporttool.back.domain;

import javaslang.Tuple;
import javaslang.Tuple3;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Nick Barban.
 */

public class ApplicationEntity {

    private final List<Tuple3<String, Object, ? extends Class>> data;

    public ApplicationEntity(HashMap<String, Object> data) {
        this.data = data.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .map(entry -> Tuple.of(entry.getKey(), entry.getValue(), entry.getValue() == null ? Object.class : entry.getValue().getClass()))
                .collect(Collectors.toList());
    }

    /*public String getColumnNames() {
        return this.data.stream()
                .filter(tuple -> tuple._2 != null)
                .map(tuple -> tuple._1)
                .collect(Collectors.joining(", "));
    }*/

    /*public List<Object> getValues() {
        return this.data.stream()
                .filter(tuple -> tuple._2 != null)
                .map(tuple -> tuple._2)
                *//*.map(tuple -> {
                    if (tuple._3.getName().equalsIgnoreCase(Number.class.getName())) {
                        return wrap(String.valueOf(tuple._2));
                    } else if (tuple._3.getName().equalsIgnoreCase(Timestamp.class.getName())) {
                        return wrap(Timestamp.valueOf(String.valueOf(tuple._2)).toString());
                    } else if (tuple._3.getName().equalsIgnoreCase(Date.class.getName())) {
                        return wrap(Date.valueOf(String.valueOf(tuple._2)).toString());
                    } else {
                        return wrap(String.valueOf(tuple._2));
                    }
                })*//*.collect(Collectors.toList());
    }*/

    /*private String wrap(String string) {
        return "\'" .concat(string).concat("\'");
    }*/

    public List<Tuple3<String, Object, ? extends Class>> getData() {
        return data;
    }
}
