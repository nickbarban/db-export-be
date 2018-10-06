package com.dbexporttool.back.domain;

import com.dbexporttool.back.enums.DbType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Nick Barban.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public final class ApplicationDataBase {
    private String url;
    private String user;
    private String password;
    private String driver;
    private String dbName;
    private DbType dbType;
}
