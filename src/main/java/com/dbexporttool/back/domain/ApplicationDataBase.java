package com.dbexporttool.back.domain;

import com.dbexporttool.back.enums.DbType;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Nick Barban.
 */
@Data
@AllArgsConstructor
public final class ApplicationDataBase {
    private final String url;
    private final String user;
    private final String password;
    private final String driver;
    private final String dbName;
    private final DbType dbType;
}
