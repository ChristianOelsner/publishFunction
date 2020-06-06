package org.example;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PublishFunction implements Function<String, Void> {

    @Override
    public Void process(String input, Context context) {
        String publishTopic = (String) context.getUserConfigValueOrDefault("publish-topic", "elasticSink");
        String dead_letter_topic = (String) context.getUserConfigValueOrDefault("publish-topic", "dead_letter_topic");

        if (isJson(input)) {
            try {
                context.newOutputMessage(publishTopic, Schema.STRING).value(input).sendAsync();
            } catch (PulsarClientException e) {
                context.getLogger().error(e.toString());
            }
        } else
        {
            try {
                context.newOutputMessage(dead_letter_topic, Schema.STRING).value(input).sendAsync();
            } catch (PulsarClientException e) {
                context.getLogger().error(e.toString());
            }
        }

        return null;
    }

    //Check if input is valid json
    public static boolean isJson(String Json) {
        try {
            new JSONObject(Json);
        } catch (JSONException ex) {
            try {
                new JSONArray(Json);
            } catch (JSONException ex1) {
                return false;
            }
        }
        return true;
    }
    /*
    Uncomment main below to deubg from IDE console
     */
//    public static void main(String[] args) throws Exception {
//        FunctionConfig functionConfig = new FunctionConfig();
//        functionConfig.setName("nettyTopicTest");
//        functionConfig.setInputs(Collections.singleton("nettyTopicTest"));
//        functionConfig.setClassName(PublishFunction.class.getName());
//        functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
//        functionConfig.setOutput("Publish");
//
//        LocalRunner localRunner = LocalRunner.builder().functionConfig(functionConfig).build();
//        localRunner.start(false);
//    }
}
