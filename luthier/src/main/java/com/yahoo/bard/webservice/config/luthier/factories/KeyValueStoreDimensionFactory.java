package com.yahoo.bard.webservice.config.luthier.factories;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.bard.webservice.config.luthier.Factory;
import com.yahoo.bard.webservice.config.luthier.LuthierIndustrialPark;
import com.yahoo.bard.webservice.data.config.LuthierDimensionField;
import com.yahoo.bard.webservice.data.dimension.Dimension;
import com.yahoo.bard.webservice.data.dimension.DimensionField;
import com.yahoo.bard.webservice.data.dimension.KeyValueStore;
import com.yahoo.bard.webservice.data.dimension.SearchProvider;
import com.yahoo.bard.webservice.data.dimension.impl.KeyValueStoreDimension;
import com.yahoo.bard.webservice.util.EnumUtils;

import java.util.LinkedHashSet;
import java.util.ArrayList;
import java.util.List;


public class KeyValueStoreDimensionFactory implements Factory<Dimension> {

    /**
     * Build a dimension instance.
     *
     * @param name  the config dictionary name (normally the apiName)
     * @param configTable  the json tree describing this config entity
     * @param resourceFactories  the source for locating dependent objects
     *
     * @return  A newly constructed config instance for the name and config provided
     */
    @Override
    public Dimension build(String name, ObjectNode configTable, LuthierIndustrialPark resourceFactories) {
        String dimensionName = name;
        String longName = configTable.get("longName").textValue();
        String category = "UNKNOWN_CATEGORY";
        String description = configTable.get("description").textValue();            // TODO: Magic values!
        KeyValueStore keyValueStore = resourceFactories.getKeyValueStore(
                configTable.get("description").textValue()
        );
        SearchProvider searchProvider = resourceFactories.getSearchProvider(
                configTable.get("searchProvider").textValue()
        );
        LinkedHashSet<DimensionField> dimensionFields = new LinkedHashSet<>();
        for(JsonNode node : configTable.get("fields")) {
            List<String> tags = new ArrayList<>();
            for (final JsonNode strNode : node.get("tags")) {
                tags.add( strNode.textValue() );
            }
            dimensionFields.add(
                    new LuthierDimensionField(
                            EnumUtils.camelCase( node.get("name").textValue() ),
                            "Error: currently there is no description",             // TODO: Magic values!
                            tags)
            );
        }
        boolean isAggregatable = true;                                              // TODO: Magic values!
        LinkedHashSet<DimensionField> defaultDimensionFields = dimensionFields;     // TODO: include this in Lua configs

        Dimension dimension = new KeyValueStoreDimension(
                dimensionName,
                longName,
                category,
                description,
                dimensionFields,
                keyValueStore,
                searchProvider,
                defaultDimensionFields,
                isAggregatable
        );

        return dimension;
    }
}
