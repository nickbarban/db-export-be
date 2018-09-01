package com.dbexporttool.back.service;

import com.dbexporttool.back.dto.RequestDTO;

/**
 * Business logic for .
 *
 * @author Nick Barban.
 */
public interface ExportService {
    void export(RequestDTO requestDTO);
}
