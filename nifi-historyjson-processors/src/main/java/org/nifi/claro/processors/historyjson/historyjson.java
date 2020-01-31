/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.nifi.claro.processors.historyjson;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;


import org.json.*;

import java.io.OutputStream;
import java.util.*;
import java.lang.Object;

import org.apache.nifi.serialization.record.util.DataTypeUtils;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class historyjson extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Reader")
            .displayName("Record Reader")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("Writer")
            .displayName("Record Writer")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("Fields")
            .displayName("Add Fields")
            .description("Agregar los campos que formaran el historial (separados por coma)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(RECORD_READER);
        descriptors.add(RECORD_WRITER);
        descriptors.add(FIELDS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final RecordReaderFactory factory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final String fields = context.getProperty(FIELDS).getValue();
        final ComponentLog logger = getLogger();
        final FlowFile outFlowFile = session.create(flowFile);
        try {
            session.read(flowFile, in -> {
                try (RecordReader reader = factory.createRecordReader(flowFile, in, logger)) {
                    final RecordSchema writeSchema = writerFactory.getSchema(null, reader.getSchema());
                    Record record;
                    final OutputStream outStream = session.write(outFlowFile);
                    final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, outStream);
                    writer.beginRecordSet();
                    // esto es solo para una prueba con schema
                    final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().id(123456L).version(1).build();
                    createRecordSchema(schemaIdentifier);
                    final List<HashMap<String, String>> listOfMaps = new ArrayList<HashMap<String, String>>();
                    final HashMap<String, String> values = new HashMap<>();
                    //Bloque while tiene pruebas y comentadas
                    while ((record = reader.nextRecord()) != null) {
                        listOfMaps.clear();
                        values.clear();
                        //getLogger().error( "#### Valor: " + record.getAsArray( "History" ));
                        List value = Arrays.asList(record.toMap());
                        //getLogger().info( "#### value: " + Arrays.toString( record.getAsArray( "History" ) ) );
                        getLogger().info("#### VALUES: " + value);
                        //record.setMapValue( "History","E_Formatted","123456" );
                        //record.setArrayValue( "History",3,"[{E_Formatted_Date=12345, E_Error_Description=Account not found , E_Formatted=E, E_Error_Code=987}]" );
                        getLogger().info("#### value: " + record.getAsString("History"));
                        String[] src = {"MapRecord[{E_Formatted=E, E_Error_Code=987}]", "MapRecord[{E_Formatted=E, E_Error_Code=987}]"};

                        //final Object childObject = record.getValue("History");
                        //final Record firstChildRecord = (Record) childObject;

                        //record.setArrayValue( "History",0,src);
                        getLogger().info("#### VALUES ORIGINAL: " + record.getValue("History"));

                        //List<Object> val = value == null ? null : Arrays.asList( value );
                        //getLogger().info( "#### List: " + val);

                        //Mantiene el historico y mapea los campos indicados por paramentro en el history
                        createHistory(fields, record, listOfMaps, values, logger);
                        record.setValue("History", listOfMaps);
                        getLogger().info("#### VALUES: " + record.getValue("History"));
                        writer.write(record);

                    }
                    final WriteResult writeResult = writer.finishRecordSet();
                    writer.close();
                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                    attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                    session.putAllAttributes(outFlowFile, attributes);
                    session.transfer(outFlowFile, REL_SUCCESS);
                } catch (final SchemaNotFoundException | MalformedRecordException e) {
                    getLogger().error("SchemaNotFound or MalformedRecordException \n" + e.toString());
                    throw new ProcessException(e);
                } catch (final Exception e) {
                    getLogger().error(e.toString());
                    throw new ProcessException(e);
                }
            });
        } catch (final Exception e) {
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with cache when processing {} due to {}", new Object[]{flowFile, e});
            return;
        }
        session.remove(flowFile);
    }

    private void createHistory(String fields, Record record, final List<HashMap<String, String>> listOfMaps,
                               final HashMap<String, String> values, ComponentLog logger) {

        if(record.getSchema().getField("History").isPresent()){
            //JSONObject jsonObjArray = new JSONObject(record.getValue("History"));
            logger.warn("NADAAAAA");
        }

        String[] array = fields.split(",");
        for (String s : array) {
            values.put(s, record.getValue(s).toString());
        }
        listOfMaps.add(values);
    }

    private RecordSchema createRecordSchema(final SchemaIdentifier schemaIdentifier) {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("E_Formatted", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("E_Error_Code", RecordFieldType.STRING.getDataType()));
        return new SimpleRecordSchema(fields, schemaIdentifier);
    }


}