package com.dbexporttool.back.dto;

import com.dbexporttool.back.domain.ApplicationDataBase;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Nick Barban.
 */
@Data
@AllArgsConstructor
public class RequestDTO {
    private final ApplicationDataBase srcDb;
    private final ApplicationTable table;
    private final ApplicationDataBase destDb;
    private final Long srcId;
}
