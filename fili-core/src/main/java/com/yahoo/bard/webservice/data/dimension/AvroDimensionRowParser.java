// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.data.dimension;

import com.fasterxml.jackson.core.JsonFactory;
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
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Parses an AVRO file into Dimension Rows.
 */
public class AvroDimensionRowParser {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDimensionRowParser.class);
    private static ObjectMapper MAPPER = null;

    private DimensionFieldMapper dimensionFieldMapper;
    private Map<String, String> convertedDimensionFields;

    /**
     * Constructs an AvroDimensionRowParser object based on the DimensionFieldMapper object.
     *
     * @param dimensionFieldMapper Object that defines the dimension field name transformations
     * @param objectMapper Object that is used to construct mapper objects
     */
    public AvroDimensionRowParser(DimensionFieldMapper dimensionFieldMapper, ObjectMapper objectMapper) {
        this.dimensionFieldMapper = dimensionFieldMapper;
        MAPPER = objectMapper;
    }

    /**
     * Validates the schema of the given AVRO file with the Dimension schema configured by the user.
     *
     * @param dimension The dimension object used to configure the dimension
     * @param avroSchemaPath The path of the AVRO schema file (.avsc)
     *
     * @return true if the schema is valid, false otherwise
     *
     * @throws NullPointerException thrown if JSON object `fields` is not present
     * @throws IOException thrown if there is error parsing the avro files
     */
    public boolean isSchemaValid(Dimension dimension, String avroSchemaPath) throws NullPointerException, IOException {

        // Convert the AVRO schema file into JsonNode Object
        JsonNode jsonAvroSchema = convertFileToJsonNode(avroSchemaPath);

        // If the JSON schema file is corrupted/ empty, then return the schema is invalid
        if (jsonAvroSchema == null) {
            return false;
        }

        // This may throw a NullPointerException, if the AVRO schema file does not have a `fields` JSON object.
        int fieldSize = jsonAvroSchema.get("fields").size();

        jsonAvroSchema = jsonAvroSchema.get("fields");

        HashSet<String> avroFields = new HashSet<>(fieldSize);

        // Streaming cannot be used, because `avroFields` is a JsonNode object
        for (int i = 0 ; i < fieldSize; i++) {
            // Parsing the JsonNode object as Text, `toString()` adds an additional `"`
            avroFields.add(jsonAvroSchema.get(i).get("name").asText());
        }

        Function<DimensionField, String> curriedConverter = curry(dimension, dimensionFieldMapper);

        // Generates a map of configured dimension field names and the converted field names, for easier lookups in the
        // `parseAvroFileDimensionRows()`
        convertedDimensionFields = dimension.getDimensionFields()
                .stream()
                .collect(Collectors.toMap(dimensionField -> dimensionField.getName(), curriedConverter));

        // Returns true, only if all the dimension fields present in the configured dimension exist in the AVRO schema
        return convertedDimensionFields.values().containsAll(avroFields);
    }

    /**
     * Parses the avro file and populates the dimension rows after validating the schema.
     *
     * @param dimension The dimension object used to configure the dimension
     * @param avroFilePath The path of the AVRO data file (.avro)
     * @param avroSchemaPath The path of the AVRO schema file (.avsc)
     *
     * @throws NullPointerException thrown if JSON object `fields` is not present
     * @throws IOException thrown if there is error parsing the avro files
     */
    public void parseAvroFileDimensionRows(Dimension dimension, String avroFilePath, String avroSchemaPath)
        throws NullPointerException, IOException {

        if (isSchemaValid(dimension, avroSchemaPath)) {

            JsonNode jsonAvroSchema = convertFileToJsonNode(avroSchemaPath);

            // Creates an AVRO schema object based on the parsed AVRO schema file (.avsc)
            Schema schema = new Schema.Parser().parse(jsonAvroSchema.toString());

            // Creates an AVRO DatumReader object based on the AVRO schema object
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            // Creates an AVRO DataFileReader object that reads the AVRO data file one record at a time
            DataFileReader<GenericRecord> dataFileReader =
                new DataFileReader<>(new File(avroFilePath), datumReader);

            // Represents each record in the avro data file
            GenericRecord avroData = null;

            while (dataFileReader.hasNext()) {
                avroData = dataFileReader.next(avroData);

                // Creates a JsonObject from the AVRO GenericRecord object
                JsonNode avroDataJson = MAPPER.readTree(new JsonFactory().createParser(avroData.toString()));

                // Constructs a Map of the converted dimension field name and the corresponding value in the avro file
                Map<String, String> dimensionFieldKeyValues = dimension.getDimensionFields()
                        .stream()
                        .collect(Collectors.toMap(dimensionField -> dimensionField.getName(),
                                                  // Looks up converted field name from the `convertedDimensionFields`
                                                  // Parsed as text to avoid the extra quotes around string objects
                                                  dimensionField -> avroDataJson
                                                        .get(convertedDimensionFields
                                                                .get(dimensionField.getName())
                                                        ).asText()
                                                )
                        );

                // Parses the dimensionRow based on the map generated above and then adds it to the dimension cache
                dimension.addDimensionRow(dimension.parseDimensionRow(dimensionFieldKeyValues));
            }
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
        File file;
        JsonNode jsonNode;
        try {
            file = new File(avroSchemaPath);
            jsonNode = MAPPER.readTree(file);
            return jsonNode;
        } catch (JsonProcessingException e) {
            LOG.error("Unable to process the Json ", e.getStackTrace());
            throw e;
        } catch (IOException e) {
            LOG.error("Unable to process the file ", e.getStackTrace());
            throw e;
        }
    }

    /**
     * Returns a converter function for the dimension field name mapping.
     *
     * @param dimension The dimension object used to configure the dimension
     * @param dimensionFieldMapper Object that defines the dimension field name transformations
     *
     * @return Function that converts the dimension field name based on the user mapping
     */
    private Function<DimensionField, String> curry(Dimension dimension, DimensionFieldMapper dimensionFieldMapper) {
        return dimensionField -> dimensionFieldMapper.converter(dimension, dimensionField);
    }
}
