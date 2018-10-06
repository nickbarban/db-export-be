package com.dbexporttool.back.dto;

import com.dbexporttool.back.domain.ApplicationDataBase;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Nick Barban.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RequestDTO {
    private ApplicationDataBase srcDb;
    private ApplicationTable table;
    private ApplicationDataBase destDb;
    private Long srcId;
}
