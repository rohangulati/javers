package org.javers.common.scanner;

import org.javers.model.mapping.FieldBasedPropertyScanner;
import org.javers.model.mapping.PropertiesAssert;
import org.javers.model.mapping.Property;
import org.javers.model.mapping.type.TypeMapper;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.javers.test.builder.TypeMapperTestBuilder.typeMapper;

/**
 * @author pawel szymczyk
 */
public class FieldBasedScannerTest extends PropertyScannerTest {

    @Before
    public void setUp() {
        TypeMapper typeMapper = typeMapper().registerAllDummyTypes().build();
        propertyScanner = new FieldBasedPropertyScanner(typeMapper);
    }

    @Test
    public void shouldScanPrivateFields() {
        //when
        List<Property> properties = propertyScanner.scan(ManagedClass.class);

        //then
        PropertiesAssert.assertThat(properties).hasProperty("privateProperty");
    }
}
