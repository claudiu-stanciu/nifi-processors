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

import org.apache.commons.io.IOUtils;

import org.apache.nifi.components.PropertyDescriptor;

import org.apache.nifi.flowfile.FlowFile;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;


import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;


import org.apache.nifi.processor.util.StandardValidators;
import org.json.JSONObject;
import org.json.JSONException;
import org.json.XML;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class XmlToJson extends AbstractProcessor {

    public static final PropertyDescriptor PROP_PP_INDENT_FACTOR = new PropertyDescriptor.Builder()
            .name("JSON pretty print indent factor")
            .description("Pretty print indent factor")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .expressionLanguageSupported(true)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original XML file")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully converted to JSON")
            .build();

    public static final Relationship REL_FAILED = new Relationship.Builder()
            .name("failed")
            .description("Failed to parse XML file. Original FlowFile will be routed on this path")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROP_PP_INDENT_FACTOR);
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
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null) {
            return;
        }
        session.transfer(originalFlowFile, REL_ORIGINAL);

        if (originalFlowFile.getSize() > 0L) {
            final AtomicReference<String> value = new AtomicReference<>();
            session.read(originalFlowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    try {
                        String xml = IOUtils.toString(in);
                        try {
                            JSONObject xmlJSONObj = XML.toJSONObject(xml);
                            final int prettyPrintIndent = context.getProperty(PROP_PP_INDENT_FACTOR).asInteger();
                            value.set(xmlJSONObj.toString(prettyPrintIndent));

                        } catch (JSONException ex) {
                            ex.printStackTrace();
                            getLogger().error("Failed to parse due to" + ex.toString() + ". Routing to failure");
                            session.transfer(originalFlowFile, REL_FAILED);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        getLogger().error("Failed to parse due to " + ex.toString() + ". Routing to failure");
                        session.transfer(originalFlowFile, REL_FAILED);
                    }
                }
            });
            // Write JSON content to FlowFile
            FlowFile flowFile = session.write(originalFlowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(value.get().getBytes());
                }
            });
            session.transfer(flowFile, REL_SUCCESS);
        } else{
            session.transfer(originalFlowFile, REL_SUCCESS);
        }

    }
}
