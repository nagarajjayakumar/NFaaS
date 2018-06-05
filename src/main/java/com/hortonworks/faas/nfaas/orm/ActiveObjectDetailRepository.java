package com.hortonworks.faas.nfaas.orm;


import com.hortonworks.faas.nfaas.dto.ActiveObjectDetail;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.repository.CrudRepository;

import javax.transaction.Transactional;
import java.util.List;

/**
 * A DAO for the entity User is simply created by extending the CrudRepository
 * interface provided by spring. The following methods are some of the ones
 * available from such interface: save, delete, deleteAll, findOne and findAll.
 * The magic is that such methods must not be implemented, and moreover it is
 * possible create new query methods working only by defining their signature!
 *
 * @author njayakumar
 */
@Configuration
@Transactional
public interface ActiveObjectDetailRepository extends CrudRepository<ActiveObjectDetail, Long> {

    public ActiveObjectDetail findById(Long id);

    public List<ActiveObjectDetail> findAllByHaoid(Long haoid);

}
