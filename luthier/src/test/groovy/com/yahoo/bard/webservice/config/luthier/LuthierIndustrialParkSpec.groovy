package com.yahoo.bard.webservice.config.luthier


import com.yahoo.bard.webservice.config.luthier.factories.KeyValueStoreDimensionFactory
import com.yahoo.bard.webservice.data.config.LuthierDimensionField
import com.yahoo.bard.webservice.data.config.ResourceDictionaries
import com.yahoo.bard.webservice.data.dimension.Dimension
import com.yahoo.bard.webservice.data.dimension.MapStore
import com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider
import spock.lang.Specification

class LuthierIndustrialParkSpec extends Specification {
    LuthierIndustrialPark industrialPark
    LuthierIndustrialPark defaultIndustrialPark
    ResourceDictionaries resourceDictionaries
    void setup() {
        resourceDictionaries = new ResourceDictionaries()
    }

    def "An industrialPark instance built with a custom dimensionFactories map contains a particular testDimension."() {
        given:
            Map<String, Factory<Dimension>> dimensionFactoriesMap = new HashMap<>()
            dimensionFactoriesMap.put("KeyValueStoreDimension", new KeyValueStoreDimensionFactory())
            industrialPark = new LuthierIndustrialPark.Builder(resourceDictionaries)
                .withDimensionFactories(dimensionFactoriesMap)
                .build()
        when:
            industrialPark.load()
            Dimension testDimension = industrialPark.getDimension("testDimension")
            testDimension.getFieldByName("nonExistentField")
        then:
            testDimension != null
            testDimension.getApiName() == "testDimension"
            testDimension.getFieldByName("testPk") == new LuthierDimensionField(
                    "testPk",
                    "TEST_PK",
                    ["primaryKey"]
            )
            thrown(IllegalArgumentException)
    }

    def "An industrialPark contains appropriate default factory maps to supply the testDimension"() {
        given:
            defaultIndustrialPark = new LuthierIndustrialPark.Builder(resourceDictionaries).build()
        when:
            defaultIndustrialPark.load()
            Dimension testDimension = defaultIndustrialPark.getDimension("testDimension")
            LuceneSearchProvider luceneSearchProvider = testDimension.getSearchProvider()
            LuceneSearchProvider expectedSearchProvider = new LuceneSearchProvider("/tmp/lucene/", 100000, 600000)
            expectedSearchProvider.setKeyValueStore(new MapStore())
            expectedSearchProvider.setDimension(testDimension)
        then:
            luceneSearchProvider.getDimension() == testDimension
            // luceneSearchProvider == expectedSearchProvider
            luceneSearchProvider.class.canonicalName ==
                    "com.yahoo.bard.webservice.data.dimension.impl.LuceneSearchProvider"
    }
}
