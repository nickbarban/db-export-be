package com.dbexporttool.back.resource;

import com.dbexporttool.back.dto.RequestDTO;
import com.dbexporttool.back.service.ExportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
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

    @Autowired
    private ExportService service;

    @GetMapping("/export")
    public void export(@RequestBody @Valid RequestDTO requestDTO) {
        service.export(requestDTO);
    }
}
