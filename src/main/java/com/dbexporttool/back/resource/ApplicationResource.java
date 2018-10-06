package com.dbexporttool.back.resource;

import com.dbexporttool.back.dto.RequestDTO;
import com.dbexporttool.back.service.ExportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

/**
 * @author Nick Barban.
 */
@RestController
@RequestMapping("/api")
public class ApplicationResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationResource.class);

    @Autowired
    private ExportService service;

    @PostMapping("/export")
    public void export(@RequestBody @Valid RequestDTO requestDTO) {
        LOGGER.info("Export with config:{}", requestDTO);

        service.export(requestDTO);
    }
}
