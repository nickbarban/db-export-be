package com.dbexporttool.back.dao;

import com.dbexporttool.back.domain.ApplicationEntity;
import org.dom4j.tree.AbstractEntity;

import java.util.Map;

/**
 * Data Access Object for {@link AbstractEntity}.
 *
 * @author Nick Barban.
 */
public interface AbstractDao {

    Map<String, Object> get(Long id);

    void persist(ApplicationEntity entity);
}
