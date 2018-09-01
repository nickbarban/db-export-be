package com.dbexporttool.back.dao;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.dbexporttool.back.domain.ApplicationDataBase;
import com.dbexporttool.back.domain.ApplicationEntity;
import com.dbexporttool.back.dto.ApplicationTable;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Implementation for {@link AbstractHibernateDao}.
 *
 * @author Nick Barban.
 */

public class CassandraDaoImpl implements AbstractDao {

    private final static Logger LOG = LoggerFactory.getLogger(CassandraDaoImpl.class);
    private static final String HOST_PORT_DELIMETER = ":";
    private static final Pattern CASSANDRA_URL_PATTERN = Pattern.compile(".+:\\d+");

    private final String srcTable;
    private final String idName;
    private final ApplicationDataBase config;
    private Cluster cluster;
    private Session session;

    public CassandraDaoImpl(ApplicationTable table, ApplicationDataBase dataBase) {
        this.srcTable = table.getName();
        this.idName = table.getIdName();
        this.config = dataBase;
        this.session = openSession(config);
    }

    private void checkUrl(String url) {
        StringUtils.isNotBlank(url);
        Assert.isTrue(url.trim().contains(HOST_PORT_DELIMETER), "Wrong url format for cassandra. It should be <host:port>");
        Assert.isTrue(CASSANDRA_URL_PATTERN.matcher(url.trim()).matches(), "Wrong url format for cassandra. It should be <host:port>");
        Assert.isTrue(StringUtils.isNumeric(url.trim().split(HOST_PORT_DELIMETER)[1]), "Wrong port format. It should be numeric");
    }

    public Map<String, Object> get(Long id) {
        Row result = session.execute("SELECT * FROM " + srcTable + " WHERE " + idName + "=" + id + ";").one();
        return result.getColumnDefinitions().asList().stream()
                .collect(Collectors.toMap(ColumnDefinitions.Definition::getName, def -> result.getObject(def.getName()), (o1, o2) -> o1));
    }

    public void persist(ApplicationEntity entity) {
        throw new NotImplementedException("Method persist is not implemented yet in CassandraDaoImpl");
    }

    protected Type getSqlType(Class type) {
        return null;
    }

    private Session openSession(ApplicationDataBase config) {
        checkUrl(config.getUrl());
        String host = config.getUrl().trim().split(HOST_PORT_DELIMETER)[0];
        Integer port = Integer.parseInt(config.getUrl().trim().split(HOST_PORT_DELIMETER)[1]);

        Cluster.Builder b = Cluster.builder().addContactPoint(host);
        b.withPort(port);
        b.withCredentials(config.getUser(), config.getPassword());
        cluster = b.build();

        return cluster.connect();
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

    public void createKeyspace(
            String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(keyspaceName).append(" WITH replication = {")
                        .append("'class':'").append(replicationStrategy)
                        .append("','replication_factor':").append(replicationFactor)
                        .append("};");

        String query = sb.toString();
        session.execute(query);
    }
}
