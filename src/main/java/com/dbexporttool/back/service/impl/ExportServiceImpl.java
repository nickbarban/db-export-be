package com.dbexporttool.back.service.impl;

import com.dbexporttool.back.dao.AbstractDao;
import com.dbexporttool.back.dao.CassandraDaoImpl;
import com.dbexporttool.back.dao.H2DaoImpl;
import com.dbexporttool.back.dao.PostgresDaoImpl;
import com.dbexporttool.back.domain.ApplicationDataBase;
import com.dbexporttool.back.domain.ApplicationEntity;
import com.dbexporttool.back.dto.ApplicationTable;
import com.dbexporttool.back.dto.RequestDTO;
import com.dbexporttool.back.enums.DbType;
import com.dbexporttool.back.service.ExportService;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Implementation for {@link ExportService}.
 *
 * @author Nick Barban.
 */
@Service
public class ExportServiceImpl implements ExportService {

    private AbstractDao srcDao;
    private AbstractDao destDao;

    @Override
    public void export(RequestDTO requestDTO) {
        srcDao = instantiateDao(requestDTO.getSrcDb(), requestDTO.getTable());
        destDao = instantiateDao(requestDTO.getDestDb(), requestDTO.getTable());

        Map<String, Object> data = srcDao.get(requestDTO.getSrcId());
        ApplicationEntity entity = new ApplicationEntity(data);
        destDao.persist(entity);
    }

    private AbstractDao instantiateDao(ApplicationDataBase dataBase, ApplicationTable table) {

        if (dataBase.getDbType().equals(DbType.CASSANDRA)) {
            return new CassandraDaoImpl(table, dataBase);
        } else if (dataBase.getDbType().equals(DbType.POSTGRES)) {
            return new PostgresDaoImpl(table, dataBase);
        } else if (dataBase.getDbType().equals(DbType.H2)) {
            return new H2DaoImpl(table, dataBase);
        } else {
            throw new NotImplementedException(String.format("DB type %s is not supported yet", dataBase.getDbType()));
        }
    }
}

