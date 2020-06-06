package org.example;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.FileNotFoundException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;


/*
A set of not very thorough tests. More to come
 */
@RunWith(MockitoJUnitRunner.class)
public class PublishFunctionTest {

    @Mock
    Context contextMock;
    PublishFunction function;
    @Test
    public void processIsJsonTest() throws FileNotFoundException, PulsarClientException {

        String testJson = "{"
                +"  \"name\": \"christian\""
                +   "}";
        String failString = "should fail misserably";

        assertTrue(function.isJson(testJson));
        assertFalse(function.isJson(failString));

    }
}