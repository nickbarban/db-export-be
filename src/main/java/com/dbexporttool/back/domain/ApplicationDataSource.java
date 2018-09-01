package com.dbexporttool.back.domain;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Nick Barban.
 */
@Data
@AllArgsConstructor
public final class ApplicationDataSource {
    private final String url;
    private final String user;
    private final String password;
    private final String driver;
    private final String dbName;
}
