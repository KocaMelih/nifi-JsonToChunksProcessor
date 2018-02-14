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
package com.trendyol.processors;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.FlowFileFilters;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Text", "Split", "Chunk", "JSON", "Modify"})
@CapabilityDescription("Gets large JSON arrays as input and divides into smaller chunks. Input assumed to be JSON array.")
public class JsonToChunksProcessor extends AbstractProcessor {

//defining the output chunk size
    public static final PropertyDescriptor CHUNK_SIZE = new PropertyDescriptor.Builder()
            .name("Chunk Size")
            .description("Output chunk size")
            .required(true)
            .defaultValue("10000")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();


    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that have been successfully processed are routed to this relationship. This includes both FlowFiles that had text"
            + " replaced and those that did not.")
        .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        //adding relationships and properties to the processor prototype
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CHUNK_SIZE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // this method will be triggered when there is a flow file on upcoming flow.
        final List<FlowFile> flowFiles = session.get(FlowFileFilters.newSizeBasedFilter(1, DataUnit.MB, 100));
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();

        //getting user defined parameters from nifi
        final int chunksize = Integer.parseInt(context.getProperty(CHUNK_SIZE).getValue());


        for (FlowFile flowFile : flowFiles) {
            final StopWatch stopWatch = new StopWatch(true);
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {
                    final ArrayList<JSONObject> results = new ArrayList<>();

                    boolean first = true;
                    //matched with the file system's block size (4KB)
                    byte[] buffer = new byte[4096];
                    int readed = 0;
                    String readedString = null;
                    String remaining = null;
                    final ArrayList records = new ArrayList();
                    while((readed = in.read(buffer)) > 0){

                        if(first){
                            readedString = new String(buffer, 1, readed - 1);
                            first = false;
                        }
                        else{
                            readedString = new String(buffer, 0, readed);
                        }

                        //if there is a remaining part from the previous reading, it shold be added to current reading
                        String toExtract = (remaining == null ) ? readedString : remaining + readedString;
                        //extracting and adding to the resultant records list
                        records.addAll(getRecords(toExtract));
                        int index = (int)records.get(records.size() - 1);

                        //if there is remaining part from the current reading, update remaining
                        if(index != toExtract.length()){
                            remaining = toExtract.substring(index);
                        }
                        else{
                            remaining = null;
                        }
                        records.remove(records.size() - 1);
                        results.addAll(records);
                        records.clear();

                        //if reached to the chunk size, create a new flow file and pass the data to it
                        if(results.size() >= chunksize){
                            FlowFile chunk = session.create();
                            chunk = session.write(chunk, new StreamCallback() {
                                @Override
                                public void process(InputStream in, OutputStream out) throws IOException {
                                    JSONArray tmpArray = new JSONArray();
                                    for(int i = 0; i < chunksize; i++){
                                        tmpArray.add(results.remove(0));

                                    }
                                    out.write(tmpArray.toJSONString().getBytes());
                                }
                            });
                            session.transfer(chunk, REL_SUCCESS);
                            logger.info(results.size() + " records transferred to success");
                        }
                    }


                    //the could be remaining jsons from the last reading
                    //so we have to route them to a new flow file
                    if(results != null && results.size() != 0){
                        FlowFile chunk = session.create();
                        chunk = session.write(chunk, new StreamCallback() {
                            @Override
                            public void process(InputStream in, OutputStream out) throws IOException {
                                JSONArray tmpArray = new JSONArray();
                                int size = results.size();
                                for(int i = 0; i < size; i++){
                                    tmpArray.add(results.remove(0));
                                }
                                out.write(tmpArray.toJSONString().getBytes());
                            }
                        });
                        session.transfer(chunk, REL_SUCCESS);
                    }
                }
            });


            //updating the reports
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            //removing the original large flow file
            session.remove(flowFile);

        }
        session.commit();
    }
/*
* reads string in a streaming fashion and returns the json objects to be added
* last object is an index pointer to the remaining part and it must be removed after the operation
* */
    private static ArrayList getRecords(final String input){
        ArrayList results = new ArrayList();
        Stack<Character> stack = new Stack<Character>();
        int firstIndex = 0;
        JSONParser jsonParser = new JSONParser();
        for(int i = 0; i < input.length(); i++){
            char c = input.charAt(i);
            if(c == '{'){
                if(stack.isEmpty()){
                    firstIndex = i;
                }
                stack.push(c);
            }
            else if(c == '}'){
                stack.pop();
            }
            if(c == ']' && i == input.length() - 1 ){
                continue;
            }
            if(stack.isEmpty() && c != ','){
                String record = input.substring(firstIndex, i + 1);
                try {
                    results.add(jsonParser.parse(record));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        int last = input.length() - 1;
        if(input.charAt(last) == '}'){
            results.add(last + 1);
        }
        else{
            results.add(firstIndex);
        }
        return results;
    }
}
