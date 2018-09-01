package com.dbexporttool.back.resource;

import com.dbexporttool.back.service.AbstractHibernateDao;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Nick Barban.
 */
@RestController
@RequestMapping("/api")
public class ApplicationResource {

    private AbstractHibernateDao postgresDao;


}
