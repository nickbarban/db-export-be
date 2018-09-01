package com.dbexporttool.back.dao;

import com.dbexporttool.back.domain.ApplicationDataBase;
import com.dbexporttool.back.domain.ApplicationEntity;
import com.dbexporttool.back.dto.ApplicationTable;
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
import org.springframework.transaction.annotation.Transactional;
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
@Transactional
public class PostgresDaoImpl extends AbstractHibernateDao {

    private final static Logger LOG = LoggerFactory.getLogger(PostgresDaoImpl.class);

    private static final String SCHEMA_DELIMETER = ".";

    public PostgresDaoImpl(ApplicationTable table, ApplicationDataBase dataBase) {
        super(table, configurate(dataBase));
    }

    private static ApplicationDataBase configurate(ApplicationDataBase config) {
        if (!config.getUrl().contains("postgresql")) {
            throw new RuntimeException(String.format("Wrong url for postgres: %s", config.getUrl()));
        }

        return config;
    }

    @Override
    public void persist(ApplicationEntity entity) {
        List<String> columnNames = entity.getData().stream()
                .map(tuple -> tuple._1)
                .collect(Collectors.toList());
        Assert.isTrue(columnNames.contains(idName), String.format("To persist entity %s id should be specified", tableName));

        session.beginTransaction();
        String tableStructure = getTableStructure(entity.getData());
        prepareSchema(session);
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
        } /*else if (!similarClasses(aClass, String.class) && !similarClasses(aClass, Character.class)) {
            result = ObjectType.INSTANCE;
        } */ else {
            result = StringType.INSTANCE;
        }

        LOG.info("SQL type for class {} is {}", aClass.getName(), result);
        return result;
    }

    private String getTableStructure(List<Tuple3<String, Object, ? extends Class>> data) {
        return data.stream()
                .map(this::prepareCreateTableColumnRow)
                .collect(Collectors.joining(", "));
    }

    private String prepareCreateTableColumnRow(Tuple3<String, Object, ? extends Class> tuple) {
        String result;

        if (tuple._1.equalsIgnoreCase(idName)) {
            result = tuple._1 + " INTEGER PRIMARY KEY";
        } else {
            result = tuple._1 + " " + getType(tuple._3);
        }

        return result;
    }

    private String getType(Class aClass) {
        String result;

        if (similarClasses(aClass, Long.class)) {
            result = "BIGINT";
        } else if (similarClasses(aClass, Integer.class)) {
            result = "INTEGER";
        } else if (similarClasses(aClass, Date.class)) {
            result = "DATE";
        } else if (similarClasses(aClass, Timestamp.class)) {
            result = "TIMESTAMP";
        } else {
            result = "TEXT";
        }

        LOG.info("SQL type for class {} is {}", aClass.getName(), result);
        return result;
    }

    private boolean similarClasses(Class aClass, Class bClass) {
        return aClass.getName().equalsIgnoreCase(bClass.getName());
    }

    private void prepareTable(Session session, String tableStructure) {
        Object table;

        if (tableName.contains(SCHEMA_DELIMETER)) {
            String schemaName = tableName.substring(0, tableName.indexOf(SCHEMA_DELIMETER));
            String tableNameWithoutSchema = tableName.substring(tableName.indexOf(SCHEMA_DELIMETER) + 1);
            LOG.info("Check if exists table with catalog={}, schema={}, name={}", config.getDbName(), schemaName, tableName);

            table = session.createNativeQuery("SELECT 1 FROM information_schema.tables WHERE table_catalog=:dbName AND table_schema=:schemaName AND table_name=:tableName")
                    .setParameter("dbName", config.getDbName())
                    .setParameter("schemaName", schemaName)
                    .setParameter("tableName", tableNameWithoutSchema)
                    .uniqueResult();
        } else {
            String schemaName = "public";
            LOG.info("Check if exists table with catalog={}, schema={}, name={}", config.getDbName(), schemaName, tableName);

            table = session.createNativeQuery("SELECT 1 FROM information_schema.tables WHERE table_catalog=:dbName AND table_schema=:schemaName AND table_name=:tableName")
                    .setParameter("dbName", config.getDbName())
                    .setParameter("tableName", tableName)
                    .uniqueResult();
        }

        if (table == null) {
            LOG.warn("Table {} does not exist in db {}. Will be created with columns: {}", tableName, config.getDbName(), tableStructure);

            session.createNativeQuery("CREATE TABLE " + tableName + "(" + tableStructure + ");")
                    .executeUpdate();
        } else {
            LOG.info("Table {} exists in db {}.", tableName, config.getDbName());
        }

    }

    private void prepareSchema(Session session) {

        if (tableName.contains(SCHEMA_DELIMETER)) {
            String schemaName = tableName.substring(0, tableName.indexOf(SCHEMA_DELIMETER));

            Object database = session.createNativeQuery("SELECT 1 FROM information_schema.schemata WHERE catalog_name=:dbName AND schema_name=:schemaName")
                    .setParameter("dbName", config.getDbName())
                    .setParameter("schemaName", schemaName)
                    .uniqueResult();

            if (database == null) {
                LOG.warn("Schema {} does not exist in db {}. Will be created", schemaName, config.getDbName());

                session.createNativeQuery("CREATE SCHEMA " + schemaName + ";")
                        .executeUpdate();
            } else {
                LOG.info("Schema {} exists in db {}.", schemaName, config.getDbName());
            }
        }

    }
}
