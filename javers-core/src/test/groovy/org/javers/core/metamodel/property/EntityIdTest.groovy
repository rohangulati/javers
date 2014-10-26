package org.javers.core.metamodel.property

import org.javers.common.exception.exceptions.JaversException
import org.javers.common.exception.exceptions.JaversExceptionCode
import org.javers.core.metamodel.clazz.Entity
import org.javers.core.metamodel.clazz.EntityDefinition
import org.javers.core.metamodel.clazz.ManagedClassFactory
import org.javers.core.model.DummyAddress
import org.javers.core.model.DummyUser
import spock.lang.Specification

/**
 * @author bartosz walacik
 */
abstract class EntityIdTest extends Specification {
    protected ManagedClassFactory entityFactory

    def "should use @id property by default"() {
        when:
        Entity entity = entityFactory.create(new EntityDefinition(DummyUser.class))

        then:
        EntityAssert.assertThat(entity).hasIdProperty("name")
    }

    def "should use custom id property when given"() {
        when:
        Entity entity = entityFactory.create(new EntityDefinition(DummyUser.class,"bigFlag"))

        then:
        EntityAssert.assertThat(entity).hasIdProperty("bigFlag")
    }

    def "should fail for Entity without Id property"() {
        when:
        entityFactory.createEntity(DummyAddress.class)

        then:
        JaversException e = thrown()
        e.code == JaversExceptionCode.ENTITY_WITHOUT_ID
    }
}
