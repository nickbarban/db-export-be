package com.dbexporttool.back.dao;

import com.dbexporttool.back.domain.ApplicationDataBase;
import com.dbexporttool.back.domain.ApplicationEntity;
import com.dbexporttool.back.dto.ApplicationTable;
import javaslang.Tuple3;
import org.hibernate.Session;
import org.hibernate.query.NativeQuery;
import org.hibernate.transform.AliasToEntityMapResultTransformer;
import org.hibernate.type.LongType;
import org.hibernate.type.StringType;
import org.hibernate.type.Type;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.orm.hibernate5.LocalSessionFactoryBean;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Data Access Object for {@link ApplicationEntity}.
 *
 * @author Nick Barban.
 */
public abstract class AbstractHibernateDao implements AbstractDao {

    protected final String tableName;
    protected final String idName;
    protected final ApplicationDataBase config;
    protected final Session session;

    protected AbstractHibernateDao(ApplicationTable table, ApplicationDataBase dataBase) {
        this.tableName = table.getName();
        this.idName = table.getIdName();
        this.config = dataBase;
        this.session = openSession();
    }

    @Override
    public Map<String, Object> get(Long id) {

        try {
            return (HashMap<String, Object>) getSession().createNativeQuery("SELECT * FROM " + tableName + " WHERE " + idName + "=:id")
                    .setParameter("id", id, LongType.INSTANCE)
                    .setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE)
                    .uniqueResult();
        } catch (Exception e) {
            String message = String.format("Can not select from %s with %s=%s", tableName, idName, id);
            throw new RuntimeException(message, e);
        }
    }

    public Session getSession() {
        return session;
    }


    private Session openSession() {
        try {
            return sessionFactoryBean().getObject().openSession();
        } catch (Exception e) {
            String message = String.format("Can not open session for url: %s", config.getUrl());
            throw new RuntimeException(message, e);
        }
    }

    private LocalSessionFactoryBean sessionFactoryBean() throws Exception {
        LocalSessionFactoryBean bean = new LocalSessionFactoryBean();
        bean.setDataSource(transactionAwareDataSourceProxy());
        bean.setPackagesToScan("com.dbexporttool.back");

        Properties properties = new Properties();
        properties.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQL9Dialect");
        properties.put("hibernate.show_sql", true);
        properties.put("hibernate.transaction.flush_before_completion", true);
        properties.put("hibernate.id.new_generator_mappings", true);

        bean.setHibernateProperties(properties);
        bean.afterPropertiesSet();

        return bean;
    }

    private TransactionAwareDataSourceProxy transactionAwareDataSourceProxy() throws Exception {
        return new TransactionAwareDataSourceProxy(dataSource());
    }

    private DataSource dataSource() {

        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.url(config.getUrl());
        dataSourceBuilder.username(config.getUser());
        dataSourceBuilder.password(config.getPassword());
        dataSourceBuilder.driverClassName(config.getDriver());

        return dataSourceBuilder.build();
    }

    protected void insert(ApplicationEntity entity) {
        List<String> insertableColumnNames = entity.getData().stream()
                .filter(tuple -> tuple._2 != null)
                .map(tuple -> tuple._1)
                .collect(Collectors.toList());
        String insertableColumnNamesString = insertableColumnNames.stream().collect(Collectors.joining(","));
        String wildcards = insertableColumnNames.stream()
                .map(name -> "?")
                .collect(Collectors.joining(","));
        NativeQuery query = session.createNativeQuery("INSERT INTO " + tableName + " (" + insertableColumnNamesString + ") VALUES (" + wildcards + ");");
        IntStream.range(0, insertableColumnNames.size())
                .forEach(i -> {
                    Tuple3<String, Object, ? extends Class> tuple = entity.getData().stream()
                            .filter(t -> t._1.equalsIgnoreCase(insertableColumnNames.get(i)))
                            .findFirst()
                            .orElseThrow(() -> new RuntimeException(String.format("Something went wrong as entity does not have column %s",
                                    insertableColumnNames.get(i))));
                    Type type = getSqlType(tuple._3);
                    Object value = type instanceof StringType ? String.valueOf(tuple._2) : tuple._2;
                    query.setParameter(i + 1, value, type);
                });

        query.executeUpdate();
    }

    protected abstract Type getSqlType(Class type);
}
