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
                    Record record;
                    final OutputStream outStream = session.write(outFlowFile);

                    final List<HashMap<String, String>> listOfMaps = new ArrayList<HashMap<String, String>>();
                    final HashMap<String, String> values = new HashMap<>();
                    final String[] fieldsArray = fields.split(",");

                    record = reader.nextRecord();//firstRecord

                    // We want to transform the first record before creating the Record Writer. We do this because the Record will likely end up with a different structure
                    // and therefore a difference Schema after being transformed. As a result, we want to transform the Record and then provide the transformed schema to the
                    // Record Writer so that if the Record Writer chooses to inherit the Record Schema from the Record itself, it will inherit the transformed schema, not the
                    // schema determined by the Record Reader.

                    if (record != null) {
                        final RecordSchema writeSchema;

                        if (!record.getSchema().getField("History").isPresent()) {
                            getLogger().debug("History field is not present");
                            Record newRecordScheme = createNewSchemeRecord(fieldsArray, record, listOfMaps, values);
                            writeSchema = writerFactory.getSchema(null, newRecordScheme.getSchema());
                        } else {
                            writeSchema = writerFactory.getSchema(null, record.getSchema());
                        }

                        RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, outStream);
                        writer.beginRecordSet();

                        //Bloque while tiene pruebas y comentadas
                        while (record != null) {
                            listOfMaps.clear();
                            values.clear();
                            //Mantiene el historico y mapea los campos indicados por paramentro en el history
                            createHistory(fieldsArray, record, listOfMaps, values, record.getValue("History"), logger);
                            record.setValue("History", listOfMaps);
                            writer.write(record);
                            record = reader.nextRecord();
                        }

                        final WriteResult writeResult = writer.finishRecordSet();
                        writer.close();

                        final Map<String, String> attributes = new HashMap<>();
                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        session.putAllAttributes(outFlowFile, attributes);
                        session.transfer(outFlowFile, REL_SUCCESS);
                    } else {
                        throw new ProcessException("No content");
                    }
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

    private final Record createNewSchemeRecord(final String[] fields, final Record record, final List<HashMap<String, String>> listOfMaps,
                                               final HashMap<String, String> values) {
        final Map<String, Object> recordMap = (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
        for (String s : fields) {
            values.put(s, null);
        }
        listOfMaps.add(values);
        recordMap.put("History", listOfMaps);
        final Record updatedRecord = DataTypeUtils.toRecord(recordMap, "r");
        return updatedRecord;
    }

    private void createHistory(final String[] fields, Record record, final List<HashMap<String, String>> listOfMaps,
                               final HashMap<String, String> values, Object obj, ComponentLog logger) {
        getLogger().debug("Creating history...");
        if (obj instanceof Object[] && ((Object[]) obj).length >= 1) {
            getLogger().debug("History array present in the registry...");
            final Object[] arrayObj = (Object[]) obj;
            for (Object o : arrayObj) {
                final List<String> tmpList = ((Record) o).getSchema().getFieldNames();
                final HashMap<String, String> tmp = new HashMap<>();
                for (final String key : tmpList) {
                    tmp.put(key, ((Record) o).getAsString(key));
                }
                listOfMaps.add(tmp);
            }
        }
        //current value mapping
        for (String s : fields) {
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
