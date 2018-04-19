package com.speechmarks.nifi.processor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

/**
 *
 * @author fozz
 */
@SideEffectFree
@Tags({"avro", "convert", "json"})
@CapabilityDescription("Replacement of ConvertAvroToJson")

@InputRequirement(Requirement.INPUT_REQUIRED)
public class ConvertAvroToClassicJSON extends AbstractProcessor {

    
    protected static final String CONTAINER_ARRAY = "array";
    protected static final String CONTAINER_NONE = "ndjson";

    private static final byte[] ZERO_LENGTH_REPRESENTATION = "".getBytes(StandardCharsets.UTF_8);

    static final PropertyDescriptor CONTAINER_OPTIONS = new PropertyDescriptor.Builder()
        .name("JSON container options")
        .description("Determines how stream of records is exposed: either as a sequence of single Objects (" + CONTAINER_NONE
            + ") (i.e. writing every Object to a new line), or as an array of Objects (" + CONTAINER_ARRAY + ").")
        .allowableValues(CONTAINER_NONE, CONTAINER_ARRAY)
        .required(true)
        .defaultValue(CONTAINER_ARRAY)
        .build();
    static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
        .name("Avro schema")
        .description("If the Avro records do not contain the schema (datum only), it must be specified here.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(false)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("A FlowFile is routed to this relationship after it has been converted to JSON")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("A FlowFile is routed to this relationship if it cannot be parsed as Avro or cannot be converted to JSON for any reason")
        .build();

    private List<PropertyDescriptor> properties;
    private volatile Schema schema = null;

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONTAINER_OPTIONS);
        properties.add(SCHEMA);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String containerOption = context.getProperty(CONTAINER_OPTIONS).getValue();
        final boolean useContainer = containerOption.equals(CONTAINER_ARRAY);


        final String stringSchema = context.getProperty(SCHEMA).getValue();
        final boolean schemaLess = stringSchema != null;

        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {
                    final GenericData genericData = GenericData.get();

                    if (schemaLess) {
                        if (schema == null) {
                            schema = new Schema.Parser().parse(stringSchema);
                        }
                        try (final InputStream in = new BufferedInputStream(rawIn);
                             final OutputStream out = new BufferedOutputStream(rawOut)) {
                            final DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                            final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
                            final GenericRecord record = reader.read(null, decoder);

                            // Schemaless records are singletons, so both useContainer and wrapSingleRecord
                            // need to be true before we wrap it with an array
                            if (useContainer) {
                                out.write('[');
                            }

                            final byte[] outputBytes = (record == null) ? ZERO_LENGTH_REPRESENTATION : genericData.toString(record).getBytes(StandardCharsets.UTF_8);
                            out.write(outputBytes);
                            
                            if ((useContainer == false) && (record != null)) {
                                out.write('\n');
                            }

                            if (useContainer) {
                                out.write(']');
                            }
                        }
                    } else {
                        try (final InputStream in = new BufferedInputStream(rawIn);
                             final OutputStream out = new BufferedOutputStream(rawOut);
                             final DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<GenericRecord>())) {

                            int recordCount = 0;
                            GenericRecord currRecord = null;
                            if (reader.hasNext()) {
                                currRecord = reader.next();
                                recordCount = (currRecord == null) ? 0 : 1;
                            }

                            // Open container if desired output is an array format and there are are multiple records or
                            // if configured to wrap single record
                            if (useContainer) {
                                out.write('[');
                            }

                            // Determine the initial output record, inclusive if we should have an empty set of Avro records
                            final byte[] outputBytes = (currRecord == null) ? ZERO_LENGTH_REPRESENTATION : genericData.toString(currRecord).getBytes(StandardCharsets.UTF_8);
                            out.write(outputBytes);

                            while (reader.hasNext()) {
                                if (useContainer) {
                                    out.write(',');
                                } else {
                                    out.write('\n');
                                }

                                currRecord = reader.next(currRecord);
                                out.write(genericData.toString(currRecord).getBytes(StandardCharsets.UTF_8));
                                recordCount++;
                            }

                            // Close container if desired output is an array format and there are multiple records or if
                            // configured to wrap a single record
                            if (useContainer) {
                                out.write(']');
                            } else {
                                if (recordCount > 0) {
                                    out.write('\n');
                                }
                            }
                        }
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to convert {} from Avro to JSON due to {}; transferring to failure", new Object[]{flowFile, pe});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(flowFile, REL_SUCCESS);
    }
    
    
}
