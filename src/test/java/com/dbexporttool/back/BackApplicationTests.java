package com.dbexporttool.back;

import com.dbexporttool.back.domain.ApplicationDataSource;
import com.dbexporttool.back.domain.ApplicationEntity;
import com.dbexporttool.back.service.AbstractHibernateDao;
import com.dbexporttool.back.service.impl.CassandraDaoImpl;
import com.dbexporttool.back.service.impl.H2DaoImpl;
import com.dbexporttool.back.service.impl.PostgresDaoImpl;
import org.assertj.core.api.Assertions;
import org.hibernate.transform.AliasToEntityMapResultTransformer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BackApplicationTests {

    @Test
    public void contextLoads() {
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
        ApplicationDataSource config = new ApplicationDataSource(url, user, password, driver, dbName);
        AbstractHibernateDao dao = new PostgresDaoImpl(srcTable, idName, config);

        HashMap<String, Object> actual = dao.get(givenId);

        Assertions.assertThat(actual).isNotNull().containsEntry("user_id", givenId.intValue());

        System.out.println(new String(new char[100]).replace('\0', 'T'));
        actual.keySet().forEach(k -> System.out.append(k).append(":").append(String.valueOf(actual.get(k))).append(":").println(actual.get(k).getClass().getName()));
        System.out.println(new String(new char[100]).replace('\0', 'T'));
    }

    @Test
    public void shouldGetUserEntityFromCassandra() {
        String url = "10.0.145.41:9042";
        String user = "nbarban";
        String password = "Pass32Word";
//        String driver = "org.postgresql.Driver";
        String srcTable = "millhouse.drivers";
        String idName = "user_id";
//        String dbName = "integration";
        Long givenId = 211L;
        ApplicationDataSource config = new ApplicationDataSource(url, user, password, null, null);
        CassandraDaoImpl dao = new CassandraDaoImpl(srcTable, idName, config);

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

        ApplicationDataSource config = new ApplicationDataSource(url, user, password, driver, dbName);
        AbstractHibernateDao dao = new PostgresDaoImpl(srcTable, idName, config);

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

        ApplicationDataSource config = new ApplicationDataSource(url, user, password, driver, dbName);
        AbstractHibernateDao dao = new H2DaoImpl(srcTable, idName, config);

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

        Map<String, Object> map = actual.entrySet().stream()

                .peek(System.out::println)

                .collect(Collectors.toMap(this::formatKey, this::formatValue, (p1, p2) -> p1));
        Assertions.assertThat(map)
                .isNotNull();
    }

    private Object formatValue(Map.Entry<String, Object> entry) {
        return entry.getValue() == null ? "null" : entry.getValue() instanceof String ? ((String) entry.getValue()).replace("\"", "") : entry.getValue();
    }

    private String formatKey(Map.Entry<String, Object> entry) {
        return entry.getKey().toLowerCase().replaceAll("\"", "");
    }
}
