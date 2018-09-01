package com.dbexporttool.back.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Nick Barban.
 */
@Data
@AllArgsConstructor
public class ApplicationTable {
    private String name;
    private final String idName;
}
