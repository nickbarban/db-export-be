package com.dbexporttool.back;

import com.dbexporttool.back.dao.AbstractHibernateDao;
import com.dbexporttool.back.dao.CassandraDaoImpl;
import com.dbexporttool.back.dao.H2DaoImpl;
import com.dbexporttool.back.dao.PostgresDaoImpl;
import com.dbexporttool.back.domain.ApplicationDataBase;
import com.dbexporttool.back.domain.ApplicationEntity;
import com.dbexporttool.back.dto.ApplicationTable;
import com.dbexporttool.back.dto.RequestDTO;
import com.dbexporttool.back.enums.DbType;
import com.dbexporttool.back.service.ExportService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.assertj.core.api.Assertions;
import org.hibernate.transform.AliasToEntityMapResultTransformer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BackApplicationTests {

    @Autowired
    private ExportService exportService;

    @Test
    public void contextLoads() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String srcUrl = "10.0.145.41:9042";
        String srcUser = "nbarban";
        String srcPassword = "Pass32Word";
        ApplicationDataBase srcDb = new ApplicationDataBase(srcUrl, srcUser, srcPassword, null, null, DbType.CASSANDRA);
        String tableName = "millhouse.drivers";
        String idName = "user_id";
        ApplicationTable table = new ApplicationTable(tableName, idName);
        String destUrl = "jdbc:postgresql://localhost:5432/db_export";
        String destUser = "postgres";
        String destPassword = "master";
        String destDriver = "org.postgresql.Driver";
        String destDBName = "db_export";
        ApplicationDataBase destDb = new ApplicationDataBase(destUrl, destUser, destPassword, destDriver, destDBName, DbType.POSTGRES);
        RequestDTO dto = new RequestDTO(srcDb, table, destDb, 210L);
        String json = mapper.writeValueAsString(dto);

        System.out.println(new String(new char[100]).replace('\0', 'T'));
        System.out.println(json);
        System.out.println(new String(new char[100]).replace('\0', 'T'));

        RequestDTO value = mapper.readValue(json, RequestDTO.class);

        System.out.println(new String(new char[100]).replace('\0', 'T'));
        System.out.println(ReflectionToStringBuilder.toString(value));
        System.out.println(new String(new char[100]).replace('\0', 'T'));
    }

    @Test
    public void shouldGetUserEntityFromPostgres() {
        String url = "jdbc:postgresql://10.0.145.95:5432/integration";
        String user = "postgres";
        String password = "master";
        String driver = "org.postgresql.Driver";
        String srcTable = "millhouse.users";
        String idName = "user_id";
        String dbName = "integration";
        Long givenId = 211L;
        ApplicationDataBase config = new ApplicationDataBase(url, user, password, driver, dbName, DbType.POSTGRES);
        ApplicationTable table = new ApplicationTable(srcTable, idName);
        AbstractHibernateDao dao = new PostgresDaoImpl(table, config);

        Map<String, Object> actual = dao.get(givenId);

        Assertions.assertThat(actual).isNotNull().containsEntry("user_id", givenId.intValue());

        System.out.println(new String(new char[100]).replace('\0', 'T'));
        actual.keySet().forEach(k -> {
            if (actual.get(k) == null) {
                System.out.append(k).append(":").append("null").append(":").println("null");
            } else {
                System.out.append(k).append(":").append(String.valueOf(actual.get(k))).append(":").println(actual.get(k).getClass().getName());
            }
        });
        System.out.println(new String(new char[100]).replace('\0', 'T'));
    }

    @Test
    public void shouldGetUserEntityFromCassandra() {
        String url = "10.0.145.41:9042";
        String user = "nbarban";
        String password = "Pass32Word";
        String srcTable = "millhouse.drivers";
        String idName = "user_id";
        Long givenId = 211L;
        ApplicationDataBase config = new ApplicationDataBase(url, user, password, null, null, DbType.CASSANDRA);
        ApplicationTable table = new ApplicationTable(srcTable, idName);
        CassandraDaoImpl dao = new CassandraDaoImpl(table, config);

        Map<String, Object> actual = dao.get(givenId);

        System.out.println(new String(new char[100]).replace('\0', 'T'));
        actual.keySet().forEach(k -> System.out.append(k).append(":").append(String.valueOf(actual.get(k))).append(":").println(actual.get(k).getClass().getName()));
        System.out.println(new String(new char[100]).replace('\0', 'T'));

        Assertions.assertThat(actual).isNotNull().containsEntry("user_id", givenId);
    }

    @Test
    public void shouldPersistEntityToPostgres() {
        String url = "jdbc:postgresql://localhost:5432/db_export";
        String user = "postgres";
        String password = "master";
        String driver = "org.postgresql.Driver";
        String srcTable = "millhouse.users";
        String idName = "user_id";
        String dbName = "db_export";

        ApplicationDataBase config = new ApplicationDataBase(url, user, password, driver, dbName, DbType.POSTGRES);
        ApplicationTable table = new ApplicationTable(srcTable, idName);
        AbstractHibernateDao dao = new PostgresDaoImpl(table, config);

        HashMap<String, Object> data = new HashMap<String, Object>() {{
            put("date_created", Timestamp.valueOf("2018-06-01 08:24:18.759"));
            put("birth_date", null);
            put("last_name", "l");
            put("active", "Y");
            put("admin", "Y");
            put("created_by", null);
            put("version", 1);
            put("phone2_id", null);
            put("password", "$2a$10$7FSpZhBDpYbhIeCPbNTKVeXkcWueFNPlXMhOncU1N/TR87aT7M3XW");
            put("date_modified", Timestamp.valueOf("2018-06-01 08:24:18.759"));
            put(idName, 211);
            put("org_id", 210);
            put("modified_by", null);
            put("phone_number", "+1 828-552-1010");
            put("position", null);
            put("first_name", "f");
            put("email", "yaroslav4 @rhyta.com");
            put("username", "f.l#110");
            put("phone_id", 731);
        }};
        ApplicationEntity entity = new ApplicationEntity(data);

        dao.persist(entity);

        HashMap<String, Object> actual = (HashMap<String, Object>) dao.getSession().createNativeQuery("SELECT * FROM " + srcTable + " WHERE " + idName + "=:param")
                .setParameter("param", data.get(idName))
                .setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE)
                .uniqueResult();

        System.out.println(new String(new char[100]).replace('\0', 'T'));
        System.out.println(actual);
        actual.keySet()
                .forEach(k -> {
                    if (actual.get(k) == null) {
                        System.out.append(k).append(":").append(String.valueOf(actual.get(k)));
                    } else {
                        System.out.append(k).append(":").append(String.valueOf(actual.get(k))).println(actual.get(k).getClass().getName());
                    }
                });
        System.out.println(new String(new char[100]).replace('\0', 'T'));

        Assertions.assertThat(actual)
                .isNotNull()
                .containsAllEntriesOf(data);

        dao.getSession().beginTransaction();
        dao.getSession().createNativeQuery("DELETE FROM " + srcTable + " WHERE " + idName + "=:id")
                .setParameter("id", data.get(idName))
                .executeUpdate();
        dao.getSession().getTransaction().commit();
    }

    @Test
    public void shouldPersistEntityToH2() {
        String url = "jdbc:h2:file:~/db_export";
        String user = "sa";
        String password = "";
        String driver = "org.h2.Driver";
        String srcTable = "db_export_users";
        String idName = "user_id";
        String dbName = "db_export";

        ApplicationDataBase config = new ApplicationDataBase(url, user, password, driver, dbName, DbType.H2);
        ApplicationTable table = new ApplicationTable(srcTable, idName);
        AbstractHibernateDao dao = new H2DaoImpl(table, config);

        HashMap<String, Object> data = new HashMap<String, Object>() {{
            put("date_created", Timestamp.valueOf("2018-06-01 08:24:18.759"));
            put("birth_date", null);
            put("last_name", "l");
            put("active", "Y");
            put("admin", "Y");
            put("created_by", null);
            put("version", 1);
            put("phone2_id", null);
            put("password", "$2a$10$7FSpZhBDpYbhIeCPbNTKVeXkcWueFNPlXMhOncU1N/TR87aT7M3XW");
            put("date_modified", Timestamp.valueOf("2018-06-01 08:24:18.759"));
            put(idName, 214);
            put("org_id", 210);
            put("modified_by", null);
            put("phone_number", "+1 828-552-1010");
            put("position", null);
            put("first_name", "f");
            put("email", "yaroslav4 @rhyta.com");
            put("username", "f.l#110");
            put("phone_id", 731);
        }};
        ApplicationEntity entity = new ApplicationEntity(data);

        dao.persist(entity);

        HashMap<String, Object> actual = (HashMap<String, Object>) dao.getSession().createNativeQuery("SELECT * FROM " + srcTable + " WHERE " + idName + "=:param")
                .setParameter("param", data.get(idName))
                .setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE)
                .uniqueResult();

        System.out.println(new String(new char[100]).replace('\0', 'T'));
        System.out.println(actual);
        actual.keySet()
                .forEach(k -> {
                    if (actual.get(k) == null) {
                        System.out.append(k).append(":").println(String.valueOf(actual.get(k)));
                    } else {
                        System.out.append(k).append(":").append(String.valueOf(actual.get(k))).println(actual.get(k).getClass().getName());
                    }
                });
        System.out.println(new String(new char[100]).replace('\0', 'T'));

        dao.getSession().beginTransaction();
        dao.getSession().createNativeQuery("DROP TABLE " + srcTable)
                .executeUpdate();
        dao.getSession().getTransaction().commit();

        Assertions.assertThat(formatMap(actual))
                .isNotNull();
    }

    private Map<String, Object> formatMap(Map<String, Object> actual) {
        return actual.entrySet().stream()
                .collect(Collectors.toMap(entry -> formatKey(entry.getKey()), entry -> formatValue(entry.getValue()), (p1, p2) -> p1));
    }

    private Object formatValue(Object value) {
        return value == null ? "null" : value instanceof String ? ((String) value).replace("\"", "") : value instanceof Long ? ((Long) value).intValue() : String.valueOf(value);
    }

    private String formatKey(String key) {
        return key.toLowerCase().replaceAll("\"", "");
    }

    @Test
    public void shouldGetFromCassandraAndPersistToPostgres() {
        String srcUrl = "10.0.145.41:9042";
        String srcUser = "nbarban";
        String srcPassword = "Pass32Word";
        String tableName = "millhouse.drivers";
        String idName = "user_id";
        Long id = 200L;
        ApplicationDataBase srcConfig = new ApplicationDataBase(srcUrl, srcUser, srcPassword, null, null, null);
        ApplicationTable table = new ApplicationTable(tableName, idName);
        CassandraDaoImpl srcDao = new CassandraDaoImpl(table, srcConfig);

        Map<String, Object> src = srcDao.get(id);
        System.out.println(new String(new char[100]).replace('\0', 'T'));
        System.out.println(src.size());
        System.out.println(src);
        System.out.println(new String(new char[100]).replace('\0', 'T'));
        Assertions.assertThat(src).isNotNull().containsEntry("user_id", id);

        String destUrl = "jdbc:postgresql://localhost:5432/db_export";
        String destUser = "postgres";
        String destPassword = "master";
        String destDriver = "org.postgresql.Driver";
        String destDBName = "db_export";
        ApplicationDataBase destConfig = new ApplicationDataBase(destUrl, destUser, destPassword, destDriver, destDBName, DbType.POSTGRES);
        AbstractHibernateDao destDao = new PostgresDaoImpl(table, destConfig);
        ApplicationEntity entity = new ApplicationEntity(src);

        destDao.persist(entity);

        Map<String, Object> actual = (HashMap<String, Object>) destDao.getSession().createNativeQuery("SELECT * FROM " + tableName + " WHERE " + idName + "=:param")
                .setParameter("param", src.get(idName))
                .setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE)
                .uniqueResult();
        System.out.println(new String(new char[100]).replace('\0', 'T'));
        System.out.println(src.size());
        System.out.println(src);
        System.out.println(new String(new char[100]).replace('\0', 'T'));
        Assertions.assertThat(actual)
                .isNotNull()
                .hasSameSizeAs(src)
                .containsOnlyKeys(src.keySet().stream().toArray(String[]::new))
                .containsValues(src.values().stream().map(this::formatValue).toArray(Object[]::new));

        destDao.getSession().beginTransaction();
        destDao.getSession().createNativeQuery("DELETE FROM " + tableName + " WHERE " + idName + "=:id")
                .setParameter("id", src.get(idName))
                .executeUpdate();
        destDao.getSession().getTransaction().commit();
    }

    @Test
    public void shouldExportDataFromCassandraToPostgres() {
        String srcUrl = "10.0.145.41:9042";
        String srcUser = "nbarban";
        String srcPassword = "Pass32Word";
        String tableName = "millhouse.drivers";
        String idName = "user_id";
        ApplicationDataBase givenSrcDb = new ApplicationDataBase(srcUrl, srcUser, srcPassword, null, null, DbType.CASSANDRA);
        ApplicationTable givenSrcTable = new ApplicationTable(tableName, idName);
        String destUrl = "jdbc:postgresql://localhost:5432/db_export";
        String destUser = "postgres";
        String destPassword = "master";
        String destDriver = "org.postgresql.Driver";
        String destDBName = "db_export";
        Long givenId = 112L;
        ApplicationDataBase givenDestDb = new ApplicationDataBase(destUrl, destUser, destPassword, destDriver, destDBName, DbType.POSTGRES);
        RequestDTO given = new RequestDTO(givenSrcDb, givenSrcTable, givenDestDb, givenId);
        exportService.export(given);

        AbstractHibernateDao destDao = new PostgresDaoImpl(givenSrcTable, givenDestDb);
        Map<String, Object> actual = (HashMap<String, Object>) destDao.getSession().createNativeQuery("SELECT * FROM " + tableName + " WHERE " + idName + "=:param")
                .setParameter("param", givenId)
                .setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE)
                .uniqueResult();
        Assertions.assertThat(actual)
                .isNotNull()
                .containsEntry(idName, givenId.intValue());

        destDao.getSession().beginTransaction();
        destDao.getSession().createNativeQuery("DELETE FROM " + tableName)
                .executeUpdate();
        destDao.getSession().getTransaction().commit();
    }
}
