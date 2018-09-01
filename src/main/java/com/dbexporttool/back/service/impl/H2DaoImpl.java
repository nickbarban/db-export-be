package com.dbexporttool.back.service.impl;

import com.dbexporttool.back.domain.ApplicationDataSource;
import com.dbexporttool.back.domain.ApplicationEntity;
import com.dbexporttool.back.service.AbstractHibernateDao;
import javaslang.Tuple3;
import org.hibernate.Session;
import org.hibernate.type.BooleanType;
import org.hibernate.type.DateType;
import org.hibernate.type.IntegerType;
import org.hibernate.type.LongType;
import org.hibernate.type.StringType;
import org.hibernate.type.TimestampType;
import org.hibernate.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation for {@link AbstractHibernateDao}.
 *
 * @author Nick Barban.
 */

public class H2DaoImpl extends AbstractHibernateDao {

    private final static Logger LOG = LoggerFactory.getLogger(H2DaoImpl.class);

    public H2DaoImpl(String tableName, String idName, ApplicationDataSource config) {
        super(tableName.equalsIgnoreCase("users") ? config.getDbName() + "_" + tableName : tableName, idName, config);
    }

    @Override
    public void persist(ApplicationEntity entity) {
        List<String> columnNames = entity.getData().stream()
                .map(tuple -> tuple._1)
                .collect(Collectors.toList());
        Assert.isTrue(columnNames.contains(idName), String.format("To persist entity %s id should be specified", tableName));

        session.beginTransaction();
        String tableStructure = getTableStructure(entity.getData());
        prepareTable(session, tableStructure);

        try {
            insert(entity);
        } catch (Exception e) {
            String message = String.format("Can not persist entity in the table %s with data:%s", tableName, entity.getData());
            throw new RuntimeException(message, e);
        } finally {
            session.getTransaction().commit();
        }
    }

    @Override
    protected Type getSqlType(Class aClass) {
        Type result;

        if (similarClasses(aClass, Long.class)) {
            result = LongType.INSTANCE;
        } else if (similarClasses(aClass, Integer.class)) {
            result = IntegerType.INSTANCE;
        } else if (similarClasses(aClass, Date.class)) {
            result = DateType.INSTANCE;
        } else if (similarClasses(aClass, Timestamp.class)) {
            result = TimestampType.INSTANCE;
        } else if (similarClasses(aClass, Boolean.class)) {
            result = BooleanType.INSTANCE;
        } else {
            result = StringType.INSTANCE;
        }

        LOG.info("SQL type for class {} is {}", aClass.getName(), result);
        return result;
    }

    private String getTableStructure(List<Tuple3<String, Object, ? extends Class>> data) {
        return data.stream()
                .map(tuple -> prepareCreateTableColumnRow(tuple))
                .collect(Collectors.joining(", "));
    }

    private String prepareCreateTableColumnRow(Tuple3<String, Object, ? extends Class> tuple) {
        String result = tuple._1 + " " + getType(tuple._3);

        if (tuple._1.equalsIgnoreCase(idName)) {
            result = result + " PRIMARY KEY";
        }

        return result;
    }

    protected String getType(Class aClass) {
        String result;

        if (similarClasses(aClass, Long.class)) {
            result = "BIGINT";
        } else if (similarClasses(aClass, Integer.class)) {
            result = "INT";
        } else if (similarClasses(aClass, Date.class)) {
            result = "DATE";
        } else if (similarClasses(aClass, Timestamp.class)) {
            result = "TIMESTAMP";
        } else {
            result = "VARCHAR";
        }

        LOG.info("SQL type for class {} is {}", aClass.getName(), result);
        return result;
    }

    private boolean similarClasses(Class aClass, Class bClass) {
        return aClass.getName().equalsIgnoreCase(bClass.getName());
    }

    private void prepareTable(Session session, String tableStructure) {
        LOG.info("Check if exists table with catalog={}, name={}", config.getDbName(), tableName);

        Object table = session.createNativeQuery("SELECT 1 FROM information_schema.tables WHERE table_catalog=:dbName AND table_name=:tableName")
                .setParameter("dbName", config.getDbName())
                .setParameter("tableName", tableName)
                .uniqueResult();

        if (table == null) {
            LOG.warn("Table {} does not exist in db {}. Will be created with columns: {}", tableName, config.getDbName(), tableStructure);

            session.createNativeQuery("CREATE TABLE IF NOT EXISTS " + tableName + "(" + tableStructure + ");")
                    .executeUpdate();
        } else {
            LOG.info("Table {} exists in db {}.", tableName, config.getDbName());
        }

    }
}
