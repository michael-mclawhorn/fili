// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension

import com.fasterxml.jackson.databind.ObjectMapper
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension
import com.yahoo.bard.webservice.data.dimension.impl.ScanSearchProviderManager
import spock.lang.Specification


class AvroDimensionRowParserSpec extends Specification {
    def LinkedHashSet<DimensionField> dimensionFields
    def KeyValueStoreDimension dimension
    def AvroDimensionRowParser avroDimensionRowParser

    def setup() {
        dimensionFields = [BardDimensionField.ID, BardDimensionField.DESC]
        dimension = new KeyValueStoreDimension("foo", "desc-foo", dimensionFields, MapStoreManager.getInstance("foo"), ScanSearchProviderManager.getInstance("foo"))
        avroDimensionRowParser = new AvroDimensionRowParser(DimensionFieldMapper.underscoreSeparatedConverter(), new ObjectMapper())
    }

    def "a schema is valid for a dimension if all of the configured dimension's fields are present in the schema"() {
        expect:
        avroDimensionRowParser.isSchemaValid(dimension, "src/test/resources/avroFilesTesting/sampleData.avsc")
    }

    def "a schema which doesn't have the `fields` section throws a NullPointerException"() {
        when:
        avroDimensionRowParser.isSchemaValid(dimension,  "src/test/resources/avroFilesTesting/illegalSchema.avsc")

        then:
        thrown NullPointerException
    }

    def "a schema is invalid for a dimension if all of the configured dimension's fields are not present in the schema"() {
        given:
        dimensionFields.add(BardDimensionField.FIELD1)

        expect:
        avroDimensionRowParser.isSchemaValid(dimension, "src/test/resources/avroFilesTesting/sampleData.avsc") == false
    }

    def "Valid schema and data parses to expected rows"() {
        given:
        DimensionRow dimensionRow1 = BardDimensionField.makeDimensionRow(dimension, "12345", "bar")
        DimensionRow dimensionRow2 = BardDimensionField.makeDimensionRow(dimension, "67890", "baz")
        Set<DimensionRow> dimSet = [dimensionRow1, dimensionRow2] as Set

        when:
        avroDimensionRowParser.parseAvroFileDimensionRows(dimension, "src/test/resources/avroFilesTesting/sampleData.avro", "src/test/resources/avroFilesTesting/sampleData.avsc")

        then:
        dimension.searchProvider.findAllOrderedDimensionRows() == dimSet
    }

    def "Invalid schema - Schema file does not exist throws an IOException"() {
        when:
        avroDimensionRowParser.parseAvroFileDimensionRows(dimension, "src/test/resources/avroFilesTesting/sampleData.avro", "src/test/resources/avroFilesTesting/foo.avsc")

        then:
        thrown IOException
    }
}
