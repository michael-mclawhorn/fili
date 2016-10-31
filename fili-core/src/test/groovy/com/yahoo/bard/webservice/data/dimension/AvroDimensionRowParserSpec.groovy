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

    def "isSchema valid"() {
        expect:
        avroDimensionRowParser.isSchemaValid(dimension, "src/main/resources/sampleData.avsc") == true
    }

    def "Schema data"() {
        given:
        DimensionRow dimensionRow1 = BardDimensionField.makeDimensionRow(dimension, "12345", "bar")
        DimensionRow dimensionRow2 = BardDimensionField.makeDimensionRow(dimension, "67890", "baz")
        Set<DimensionRow> dimSet = [dimensionRow1, dimensionRow2] as Set

        when:
        avroDimensionRowParser.parseAvroFileDimensionRows(dimension,
                "src/main/resources/sampleData.avro",
                "src/main/resources/sampleData.avsc")

        then:
        dimension.searchProvider.findAllDimensionRows() == dimSet
    }
}
