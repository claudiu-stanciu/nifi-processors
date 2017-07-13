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

package com.thinkbiganalytics;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tags({"xml", "json", "convert"})
@CapabilityDescription("Converts a XML into JSON")
@ReadsAttribute(attribute = "filename", description = "The filename to use when writing the FlowFile to disk.")
public class ConvertXmlToJson extends AbstractProcessor {

    final static Logger logger = LoggerFactory.getLogger(ConvertXmlToJson.class);

    public static final PropertyDescriptor PRETTY_PRINT_INDENT_FACTOR = new PropertyDescriptor.Builder()
            .name("JSON pretty print indent factor").description("Pretty print indent factor").required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR).defaultValue("0").expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("Original XML file").build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted to JSON").build();

    public static final Relationship REL_FAILED = new Relationship.Builder().name("failed")
            .description("Failed to parse XML file. Original FlowFile will be routed on this path").build();

    private Set<Relationship> relationships;

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PRETTY_PRINT_INDENT_FACTOR);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AtomicBoolean failed = new AtomicBoolean(false);

        final FlowFile originalFlowFile = session.get();
        
        if (originalFlowFile == null || originalFlowFile.getSize() == 0L) {
            return;
        }
        final FlowFile flowFile = session.clone(originalFlowFile);

        session.transfer(originalFlowFile, REL_ORIGINAL);

        final AtomicReference<String> jsonObjVal = new AtomicReference<>();
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {
                    final int prettyPrintIndent = context.getProperty(PRETTY_PRINT_INDENT_FACTOR)
                            .evaluateAttributeExpressions(flowFile).asInteger();

                    String xml = IOUtils.toString(in, Charset.defaultCharset());

                    JSONObject xmlJSONObj = XML.toJSONObject(xml);

                    jsonObjVal.set(xmlJSONObj.toString(prettyPrintIndent));
                    getLogger().debug(jsonObjVal.get());
                    if (jsonObjVal.get().isEmpty()) {
                        failed.set(true);
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                    getLogger().error("Failed to parse to JSON due to " + ex.toString() + ". Routing to failure");
                    failed.set(true);
                }
            }
        });

        if (!failed.get()) {
            // Write JSON content to FlowFile
            FlowFile flowFileJson = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(jsonObjVal.get().getBytes());
                }
            });
            session.transfer(flowFileJson, REL_SUCCESS);
        } else {
            session.transfer(flowFile, REL_FAILED);
        }

    }

}
