/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package org.apache.jmeter.protocol.rocketmq.sampler;

import java.beans.PropertyDescriptor;

import org.apache.jmeter.testbeans.BeanInfoSupport;
import org.apache.jmeter.testbeans.gui.TypeEditor;

public class RocketMQProducerSamplerBeanInfo extends BeanInfoSupport {

    private static final String[] METHOD_TAGS = new String[3];
    static final int METHOD_SYNC = 0;
    static final int METHOD_ASYNC = 1;
    static final int METHOD_ONEWAY = 2;

    // Store the resource keys
    static {
        METHOD_TAGS[METHOD_SYNC]   = "method.sync";
        METHOD_TAGS[METHOD_ASYNC]  = "method.async";
        METHOD_TAGS[METHOD_ONEWAY] = "method.oneway";
    }

    /**
     * Construct a BeanInfo for the given class.
     *
     */
    public RocketMQProducerSamplerBeanInfo(){
        super(RocketMQProducerSampler.class);

        PropertyDescriptor p;
        p = property("nameserveraddr");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p.setValue("order",1);

        p = property("port");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, 9876);
        p.setValue("order",2);

        p = property("producergroup");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p.setValue("order",3);

        p = property("messageTopic");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p.setValue("order",4);

        p = property("messagetag");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p.setValue("order",5);

        p = property("messageBody");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, "");
        p.setValue("order",6);

        p = property("timeout");
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, 0);
        p.setValue("order",7);

        p = property("sendMethod", TypeEditor.ComboStringEditor);
        p.setValue(RESOURCE_BUNDLE, getBeanDescriptor().getValue(RESOURCE_BUNDLE));
        p.setValue(NOT_UNDEFINED, Boolean.TRUE);
        p.setValue(DEFAULT, METHOD_TAGS[METHOD_SYNC]);
        p.setValue(NOT_OTHER, Boolean.FALSE);
        p.setValue(NOT_EXPRESSION, Boolean.TRUE);
        p.setValue(TAGS, METHOD_TAGS);
        p.setValue("order",8);

    }

    public static int getMethodTypeIndex(String methodType){
        if(methodType==null) return 0;
        for (int i = 0; i < METHOD_TAGS.length; i++) {
            if(methodType.equalsIgnoreCase(METHOD_TAGS[i])){
                return i;
            }
        }

        return -1;
    }

}
