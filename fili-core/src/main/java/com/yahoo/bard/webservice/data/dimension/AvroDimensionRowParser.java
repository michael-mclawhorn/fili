// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.yahoo.bard.webservice.data.cache.HashDataCache.Pair;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Parses an AVRO file into Dimension Rows.
 */
public class AvroDimensionRowParser {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDimensionRowParser.class);
    private ObjectMapper objectMapper;

    private DimensionFieldMapper dimensionFieldMapper;

    /**
     * Constructs an AvroDimensionRowParser object based on the DimensionFieldMapper object.
     *
     * @param dimensionFieldMapper Object that defines the dimension field name transformations
     * @param objectMapper Object that is used to construct mapper objects
     */
    public AvroDimensionRowParser(DimensionFieldMapper dimensionFieldMapper, ObjectMapper objectMapper) {
        this.dimensionFieldMapper = memoize(dimensionFieldMapper);
        this.objectMapper = objectMapper;
    }

    /**
     * Validates the schema of the given AVRO file with the Dimension schema configured by the user.
     *
     * @param dimension The dimension object used to configure the dimension
     * @param avroSchemaPath The path of the AVRO schema file (.avsc)
     *
     * @return true if the schema is valid, false otherwise
     *
     * @throws IllegalArgumentException thrown if JSON object `fields` is not present
     * @throws IOException thrown if there is error parsing the avro files
     *
     * The sample schema is expected to be in the following format
     *
     * {
     *    "type" : "record",
     *    "name" : "TUPLE_0",
     *    "fields" : [
     *    {
     *    "name" : "FOO_ID",
     *    "type" : [ "null","int" ]
     *    },
     *    {
     *    "name" : "FOO_DESC",
     *    "type" : [ "null", "string" ]
     *    }
     *    ]
     * }
     *
     */
    public boolean isSchemaValid(Dimension dimension, String avroSchemaPath)
        throws IllegalArgumentException, IOException {

        // Convert the AVRO schema file into JsonNode Object
        JsonNode jsonAvroSchema = convertFileToJsonNode(avroSchemaPath);

        // If the JSON schema file is corrupted/ empty, then return the schema is invalid
        if (jsonAvroSchema == null) {
            return false;
        }

        try {
            jsonAvroSchema = jsonAvroSchema.get("fields");
        } catch (NullPointerException e) {
            String msg = "`fields` is a required field in the avro schema {}";
            LOG.error(msg, e);
            throw new IllegalArgumentException(msg);
        }

        // Populating the set of avro field names
        Set<String> avroFields = StreamSupport.stream(jsonAvroSchema.spliterator(), false)
                .map(jsonNode -> jsonNode.get("name"))
                .map(JsonNode::asText)
                .collect(Collectors.toSet());

        // True only if all of the mapped dimension fields are present in the Avro schema
        return dimension.getDimensionFields().stream()
                .map(dimensionField -> dimensionFieldMapper.convert(dimension, dimensionField))
                .allMatch(avroFields::contains);
    }

    /**
     * Parses the avro file and populates the dimension rows after validating the schema.
     *
     * @param dimension The dimension object used to configure the dimension
     * @param avroFilePath The path of the AVRO data file (.avro)
     * @param avroSchemaPath The path of the AVRO schema file (.avsc)
     *
     * @throws IllegalArgumentException thrown if JSON object `fields` is not present
     * @throws IOException thrown if there is error parsing the avro files
     */
    public void parseAvroFileDimensionRows(Dimension dimension, String avroFilePath, String avroSchemaPath)
        throws IllegalArgumentException, IOException {

        if (isSchemaValid(dimension, avroSchemaPath)) {

            // Creates an AVRO schema object based on the parsed AVRO schema file (.avsc)
            Schema schema = new Schema.Parser().parse(new File(avroSchemaPath));

            // Creates an AVRO DatumReader object based on the AVRO schema object
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            // Creates an AVRO DataFileReader object that reads the AVRO data file one record at a time
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(avroFilePath), datumReader);

            // Generates a set of dimension Rows after retrieving the appropriate fields
            Set<DimensionRow> dimensionRows = StreamSupport.stream(dataFileReader.spliterator(), false)
                    .map(genericRecord -> dimension.getDimensionFields().stream().collect(
                             Collectors.toMap(
                                 DimensionField::getName,
                                 dimensionField -> genericRecord.get(
                                     dimensionFieldMapper.convert(dimension, dimensionField)
                                 ).toString()
                             )
                         )
                    )
                    .map(dimension::parseDimensionRow)
                    .collect(Collectors.toSet());

                // Adds all dimension rows to the dimension cache
                dimension.addAllDimensionRows(dimensionRows);
        } else {
            String msg = "The AVRO schema file is corrupted / empty {}";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Constructs a JSON Node object from the avro schema file.
     *
     * @param avroSchemaPath The path of the AVRO schema file (.avsc)
     *
     * @return JsonNode object
     *
     * @throws IOException thrown if there is error parsing the avro files
     */
    private JsonNode convertFileToJsonNode(String avroSchemaPath) throws IOException {

        try {
            File file = new File(avroSchemaPath);
            JsonNode jsonNode = objectMapper.readTree(file);
            return jsonNode;
        } catch (JsonProcessingException e) {
            LOG.error("Unable to process the Json {}", e);
            throw e;
        } catch (IOException e) {
            LOG.error("Unable to process the file {}", e);
            throw e;
        }
    }

    /**
     * Returns a memoized converter function for the dimension field name mapping.
     *
     * @param dimensionFieldMapper Object that defines the dimension field name transformations
     * @return Memoized function that converts the dimension field name based on the user mapping
     */
    private DimensionFieldMapper memoize(DimensionFieldMapper dimensionFieldMapper) {
        Map<Pair<Dimension, DimensionField>, String> cache = new HashMap<>();
        return (dimension, dimensionField) -> cache.computeIfAbsent(
                new Pair<>(dimension, dimensionField),
                key -> dimensionFieldMapper.convert(key.getKey(), key.getValue())
        );
    }
}
