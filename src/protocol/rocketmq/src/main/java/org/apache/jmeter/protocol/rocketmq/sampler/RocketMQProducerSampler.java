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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.Interruptible;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testbeans.TestBean;
import org.apache.jmeter.testelement.TestElement;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jmeter.protocol.rocketmq.sampler.RocketMQProducerSamplerBeanInfo.getMethodTypeIndex;

public class RocketMQProducerSampler extends AbstractSampler implements Interruptible, TestBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQProducerSampler.class);

    private static final Set<String> APPLIABLE_CONFIG_CLASSES = new HashSet<>(
            Arrays.asList("org.apache.jmeter.config.gui.SimpleConfigGui"));

    private DefaultMQProducer producer;
    private String producergroup;
    private String nameserveraddr;
    private int port;
    private  String messageTopic;
    private  String messagetag;
    private  String messageBody;
    private  long timeout;
    private String sendMethod;

    public DefaultMQProducer getProducer() {
        return producer;
    }
    public void setProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }
    public String getProducergroup() {
        return producergroup;
    }
    public void setProducergroup(String producergroup) {
        this.producergroup = producergroup;
    }
    public String getNameserveraddr() {
        return nameserveraddr;
    }
    public void setNameserveraddr(String nameserveraddr) {
        this.nameserveraddr = nameserveraddr;
    }
    public String getMessageTopic() {
        return messageTopic;
    }
    public void setMessageTopic(String messageTopic) {
        this.messageTopic = messageTopic;
    }
    public String getMessagetag() {
        return messagetag;
    }
    public void setMessagetag(String messagetag) {
        this.messagetag = messagetag;
    }
    public String getMessageBody() {
        return messageBody;
    }
    public void setMessageBody(String messageBody) {
        this.messageBody = messageBody;
    }
    public long getTimeout() { return timeout; }
    public void setTimeout(long timeout) { this.timeout = timeout; }
    public String getSendMethod() { return sendMethod; }
    public void setSendMethod(String sendMethod) { this.sendMethod = sendMethod; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    @Override
    public SampleResult sample(Entry entry) {
        return sample();
    }

    private Message prepareMessage(){

        try {
            Message msg = new Message(messageTopic /* Topic */,
                    messagetag /* Tag */,
                    messageBody.getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            return msg;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            LOGGER.error("messageBody error",e);
        }

        return null;
    }

    //sync
    private void sendSync(SampleResult sampleResult){
            try {
                SendResult sendResult = producer.send(prepareMessage(),timeout);

                sampleResult.setDataType(SampleResult.TEXT);
                sampleResult.setResponseCodeOK();
                sampleResult.setSuccessful(true);

            } catch (Exception e) {
                LOGGER.error("rocketmq error",e);
                sampleResult.setDataType(SampleResult.TEXT);
            }
    }

    //async
    private void sendAsync(SampleResult sampleResult){

            try {
                producer.send(prepareMessage(), new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        sampleResult.setDataType(SampleResult.TEXT);
                        sampleResult.setResponseCodeOK();
                        sampleResult.setSuccessful(true);
                    }

                    @Override
                    public void onException(Throwable e) {
                        sampleResult.setDataType(SampleResult.TEXT);
                    }
                }, timeout);
            } catch (Exception e) {
                LOGGER.error("rocketmq sendAsync error",e);
                sampleResult.setDataType(SampleResult.TEXT);
            }
    }

    //oneway
    private void sendOneway(SampleResult sampleResult){

            try {
                producer.sendOneway(prepareMessage());
                sampleResult.setResponseOK();
            } catch (Exception e) {
                LOGGER.error("rocketmq error",e);
                sampleResult.setDataType(SampleResult.TEXT);
            }
    }

    private void before(SampleResult sampleResult){

        producer = new DefaultMQProducer(producergroup);

        sampleResult.setSampleLabel(getName());
        sampleResult.sampleStart();

        producer.setNamesrvAddr(nameserveraddr+":"+port);

        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
            producer.shutdown();
        }

    }

    private void after(SampleResult sr){
        producer.shutdown();
        sr.setSentBytes(0);
        sr.setHeadersSize(0);
        sr.setBodySize(0L);
        sr.sampleEnd();
    }

    private SampleResult sample(){

        LOGGER.info("rocketmq sample start");

        SampleResult sampleresult = new SampleResult();

        before(sampleresult);

        int sendMethodIndex = getMethodTypeIndex(sendMethod);

        switch (sendMethodIndex){
            case RocketMQProducerSamplerBeanInfo.METHOD_ASYNC:
                sendAsync(sampleresult);
                break;
            case RocketMQProducerSamplerBeanInfo.METHOD_ONEWAY:
                sendOneway(sampleresult);
                break;
            default:
                sendSync(sampleresult);
        }

        after(sampleresult);

        LOGGER.info("rocketmq sample end");

        return sampleresult;
    }

    @Override
    public boolean interrupt() {
        return false;
    }

    /**
     * @see org.apache.jmeter.samplers.AbstractSampler#applies(org.apache.jmeter.config.ConfigTestElement)
     */
    @Override
    public boolean applies(ConfigTestElement configElement) {
        String guiClass = configElement.getProperty(TestElement.GUI_CLASS).getStringValue();
        return APPLIABLE_CONFIG_CLASSES.contains(guiClass);
    }

}
