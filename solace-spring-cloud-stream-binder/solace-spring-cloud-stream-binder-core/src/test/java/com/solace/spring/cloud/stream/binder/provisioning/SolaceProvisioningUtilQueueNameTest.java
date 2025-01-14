package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;

import java.util.*;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SolaceProvisioningUtilQueueNameTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceProvisioningUtilQueueNameTest.class);

    public static Stream<Arguments> arguments() {
        List<List<Object>> testCases = new ArrayList<>();

        // destination
        testCases.add(new ArrayList<>(Collections.singletonList("simple/destination")));
        testCases.add(new ArrayList<>(Collections.singletonList("wildcard/*/destination/>")));

        // group
        {
            List<List<Object>> dupeList = deepClone(testCases);
            for (List<Object> testCase : testCases) {
                testCase.add(null);
            }

            for (List<Object> testCase : dupeList) {
                testCase.add("simpleGroup");
            }
            testCases.addAll(dupeList);
        }

        // queue name prefix
        {
            List<List<Object>> dupeList1 = deepClone(testCases);
            List<List<Object>> dupeList2 = deepClone(testCases);
            for (List<Object> testCase : testCases) {
                testCase.add(null);
            }

            for (List<Object> testCase : dupeList1) {
                testCase.add("custom-prefix");
            }

            for (List<Object> testCase : dupeList2) {
                testCase.add("");
            }
            testCases.addAll(dupeList1);
            testCases.addAll(dupeList2);
        }

        // useGroupNameInQueueName
        // useGroupNameInErrorQueueName
        // useFamiliarityInQueueName
        // useDestinationEncodingInQueueName
        for (int i = 0; i < 4; i++) {
            List<List<Object>> dupeList = deepClone(testCases);
            for (List<Object> testCase : testCases) {
                testCase.add(true);
            }

            for (List<Object> testCase : dupeList) {
                testCase.add(false);
            }
            testCases.addAll(dupeList);
        }

        return testCases.stream().map(List::toArray).map(Arguments::of);
    }

    private static List<List<Object>> deepClone(List<List<Object>> input) {
        List<List<Object>> cloned = new ArrayList<>();
        for (List<Object> nestedList : input) {
            cloned.add(new ArrayList<>(nestedList));
        }
        return cloned;
    }

    @ParameterizedTest(name = "[{index}] dest={0} group={1} prefix={2} useGroup={3} useGroupInEQ={4} useFamiliarity={5} useDestEnc={6}")
    @MethodSource("arguments")
    public void getQueueName(String destination, String groupName, String prefix, boolean useGroupName,
                             boolean useGroupNameInErrorQueue, boolean useFamiliarity,
                             boolean useDestinationEncoding) {
        SolaceConsumerProperties consumerProperties = createConsumerProperties(prefix, useGroupName,
                useGroupNameInErrorQueue, useFamiliarity, useDestinationEncoding);
        boolean isAnonymous = groupName == null;

        String actual = SolaceProvisioningUtil.getQueueNames(destination, groupName, new ExtendedConsumerProperties<>(consumerProperties), isAnonymous)
                .getConsumerGroupQueueName();
        LOGGER.info("Testing Queue Name: {}", actual);

        int levelIdx = 0;
        String[] levels = actual.split("/");

        if (!consumerProperties.getQueueNamePrefix().isEmpty()) {
            assertEquals(consumerProperties.getQueueNamePrefix(), levels[levelIdx]);
            levelIdx++;
        }

        if (consumerProperties.isUseFamiliarityInQueueName()) {
            assertEquals(isAnonymous ? "an" : "wk", levels[levelIdx]);
            levelIdx++;
        }

        if (isAnonymous) {
            assertThat(levels[levelIdx], matchesRegex("\\b[0-9a-f]{8}\\b(?:-[0-9a-f]{4}){3}-\\b[0-9a-f]{12}\\b"));
            levelIdx++;
        } else {
            if (consumerProperties.isUseGroupNameInQueueName()) {
                assertEquals(groupName, levels[levelIdx]);
                levelIdx++;
            }
        }

        if (consumerProperties.isUseDestinationEncodingInQueueName()) {
            assertEquals("plain", levels[levelIdx]);
            levelIdx++;
        }

        String transformedDestination;
        if (destination.contains("*") || destination.contains(">")) {
            transformedDestination = destination.replaceAll("[*>]", "_");
        } else {
            transformedDestination = destination;
        }

        for (String destinationLevel : transformedDestination.split("/")) {
            assertEquals(destinationLevel, levels[levelIdx]);
            levelIdx++;
        }
    }

    @ParameterizedTest(name = "[{index}] dest={0} group={1} prefix={2} useGroup={3} useGroupInEQ={4} useFamiliarity={5} useDestEnc={6}")
    @MethodSource("arguments")
    public void getErrorQueueName(String destination, String groupName, String prefix, boolean useGroupName,
                                  boolean useGroupNameInErrorQueue, boolean useFamiliarity,
                                  boolean useDestinationEncoding) {
        SolaceConsumerProperties consumerProperties = createConsumerProperties(prefix, useGroupName,
                useGroupNameInErrorQueue, useFamiliarity, useDestinationEncoding);
        boolean isAnonymous = groupName == null;

        String actual = SolaceProvisioningUtil.getQueueNames(destination, groupName, new ExtendedConsumerProperties<>(consumerProperties), isAnonymous)
                .getErrorQueueName();

        LOGGER.info("Testing Error Queue Name: {}", actual);

        int levelIdx = 0;
        String[] levels = actual.split("/");

        if (!consumerProperties.getQueueNamePrefix().isEmpty()) {
            assertEquals(consumerProperties.getQueueNamePrefix(), levels[levelIdx]);
            levelIdx++;
        }

        assertEquals("error", levels[levelIdx]);
        levelIdx++;

        if (consumerProperties.isUseFamiliarityInQueueName()) {
            assertEquals(isAnonymous ? "an" : "wk", levels[levelIdx]);
            levelIdx++;
        }

        if (isAnonymous) {
            assertThat(levels[levelIdx], matchesRegex("\\b[0-9a-f]{8}\\b(?:-[0-9a-f]{4}){3}-\\b[0-9a-f]{12}\\b"));
            levelIdx++;
        } else {
            if (consumerProperties.isUseGroupNameInErrorQueueName()) {
                assertEquals(groupName, levels[levelIdx]);
                levelIdx++;
            }
        }

        if (consumerProperties.isUseDestinationEncodingInQueueName()) {
            assertEquals("plain", levels[levelIdx]);
            levelIdx++;
        }

        String transformedDestination;
        if (destination.contains("*") || destination.contains(">")) {
            transformedDestination = destination.replaceAll("[*>]", "_");
        } else {
            transformedDestination = destination;
        }

        for (String destinationLevel : transformedDestination.split("/")) {
            assertEquals(destinationLevel, levels[levelIdx]);
            levelIdx++;
        }
    }

    private SolaceConsumerProperties createConsumerProperties(String prefix, boolean useGroupName,
                                                              boolean useGroupNameInErrorQueue, boolean useFamiliarity,
                                                              boolean useDestinationEncoding) {
        SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
        if (prefix != null) {
            consumerProperties.setQueueNamePrefix(prefix);
        } else {
            assertEquals("scst", consumerProperties.getQueueNamePrefix());
        }
        consumerProperties.setUseGroupNameInQueueName(useGroupName);
        consumerProperties.setUseGroupNameInErrorQueueName(useGroupNameInErrorQueue);
        consumerProperties.setUseFamiliarityInQueueName(useFamiliarity);
        consumerProperties.setUseDestinationEncodingInQueueName(useDestinationEncoding);
        return consumerProperties;
    }

    @Test
    public void testDefaultQueueNameExpressionsWithPrefixAndGroupAndDestinationContainingWhitespaces() {
        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
        consumerProperties.getExtension().setQueueNamePrefix("    aQueueNamePrefixWithSpaces    ");

        String destination = "  destination/with/spaces      ";
        String group = "    aGroupWithSpaces    ";

        SolaceProvisioningUtil.QueueNames queueNames = SolaceProvisioningUtil.getQueueNames(destination, group, consumerProperties,
                SolaceProvisioningUtil.isAnonQueue(group));

        assertEquals("aQueueNamePrefixWithSpaces/wk/aGroupWithSpaces/plain/destination/with/spaces", queueNames.getConsumerGroupQueueName());
        assertEquals("aQueueNamePrefixWithSpaces/error/wk/aGroupWithSpaces/plain/destination/with/spaces", queueNames.getErrorQueueName());
    }

    @Test
    public void testDefaultQueueNameExpressionsWithGroupAsWhiteSpacesOnlyGeneratesAGroup() {
        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(new SolaceConsumerProperties());

        String destination = "simple/destination";
        String group = "    ";

        SolaceProvisioningUtil.QueueNames queueNames = SolaceProvisioningUtil
                .getQueueNames(destination, group, consumerProperties, SolaceProvisioningUtil.isAnonQueue(group));

        assertThat(queueNames.getConsumerGroupQueueName(), matchesRegex("scst\\/an\\/\\b[0-9a-f]{8}\\b(?:-[0-9a-f]{4}){3}-\\b[0-9a-f]{12}\\b\\/plain\\/simple\\/destination"));
        assertThat(queueNames.getErrorQueueName(), matchesRegex("scst\\/error\\/an\\/\\b[0-9a-f]{8}\\b(?:-[0-9a-f]{4}){3}-\\b[0-9a-f]{12}\\b\\/plain\\/simple\\/destination"));
    }

    @Test
    public void testDefaultQueueNameExpressionsWithGroupAsNullGeneratesAGroup() {
        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(new SolaceConsumerProperties());

        String destination = "simple/destination";
        String group = null;

        SolaceProvisioningUtil.QueueNames queueNames = SolaceProvisioningUtil
                .getQueueNames(destination, group, consumerProperties, SolaceProvisioningUtil.isAnonQueue(group));

        assertThat(queueNames.getConsumerGroupQueueName(), matchesRegex("scst\\/an\\/\\b[0-9a-f]{8}\\b(?:-[0-9a-f]{4}){3}-\\b[0-9a-f]{12}\\b\\/plain\\/simple\\/destination"));
        assertThat(queueNames.getErrorQueueName(), matchesRegex("scst\\/error\\/an\\/\\b[0-9a-f]{8}\\b(?:-[0-9a-f]{4}){3}-\\b[0-9a-f]{12}\\b\\/plain\\/simple\\/destination"));
    }

    @Test
    public void testQueueNameExpressionWithStaticValue() {
        SolaceConsumerProperties consumerProperties = createConsumerProperties(null, true, true, true, true);
        //The escaped single quote '' resolves to a single quote in the actual queue name
        consumerProperties.setQueueNameExpression("'My/Static.QueueName-_''>*!@#$%^&()+=#{test}:[]{}|\\\"-~'");

        String actual = SolaceProvisioningUtil
                .getQueueNames("unused/destination", "unusedGroup", new ExtendedConsumerProperties<>(consumerProperties), true)
                .getConsumerGroupQueueName();
        assertEquals("My/Static.QueueName-_'>*!@#$%^&()+=#{test}:[]{}|\\\"-~", actual);
    }

    @Test
    public void testQueueNameExpressionWithSolaceProperties() {
        SolaceConsumerProperties consumerProperties = createConsumerProperties("myCustomPrefix", true, true, true, true);
        consumerProperties.setQueueMaxMsgRedelivery(5);

        consumerProperties.setQueueNameExpression("properties.solace.queueNamePrefix + '_' + properties.solace.useGroupNameInQueueName + '_' + properties.solace.queueMaxMsgRedelivery + '_' + properties.solace.errorQueueNameOverride");

        String actual = SolaceProvisioningUtil
                .getQueueNames("unused/destination", "unusedGroup", new ExtendedConsumerProperties<>(consumerProperties), false)
                .getConsumerGroupQueueName();
        assertEquals("myCustomPrefix_true_5_null", actual);
    }

    @Test
    public void testQueueNameExpressionWithLongFormSolaceProperties() {
        SolaceConsumerProperties consumerProperties = createConsumerProperties("myCustomPrefix", true, true, true, true);
        consumerProperties.setQueueMaxMsgRedelivery(5);

        consumerProperties.setQueueNameExpression("properties.spring.extension.queueNamePrefix + '_' + properties.spring.extension.useGroupNameInQueueName + '_' + properties.spring.extension.queueMaxMsgRedelivery + '_' + properties.spring.extension.errorQueueNameOverride");

        String actual = SolaceProvisioningUtil
                .getQueueNames("unused/destination", "unusedGroup", new ExtendedConsumerProperties<>(consumerProperties), false)
                .getConsumerGroupQueueName();
        assertEquals("myCustomPrefix_true_5_null", actual);
    }

    @Test
    public void testQueueNameExpressionWithSpringProperties() {
        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
        consumerProperties.setMaxAttempts(22);
        consumerProperties.setAutoStartup(true);
        consumerProperties.setDefaultRetryable(false);
        consumerProperties.getExtension().setQueueNameExpression("properties.spring.maxAttempts + '_' + properties.spring.autoStartup + '_' + properties.spring.defaultRetryable");

        String actual = SolaceProvisioningUtil
                .getQueueNames("simple/destination", "groupName", consumerProperties, false)
                .getConsumerGroupQueueName();
        assertEquals("22_true_false", actual);
    }

    @Test
    public void testErrorQueueNameExpressionWithSolaceAndSpringProperties() {
        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
        consumerProperties.setMaxAttempts(10);
        consumerProperties.getExtension().setQueueMaxMsgRedelivery(11);
        consumerProperties.getExtension().setErrorQueueNameExpression("properties.spring.maxAttempts + '_' + properties.solace.queueMaxMsgRedelivery + '_' + properties.spring.extension.errorQueueNameOverride");

        String actual = SolaceProvisioningUtil
                .getQueueNames("unused/destination", "unusedGroup", consumerProperties, false)
                .getErrorQueueName();
        assertEquals("10_11_null", actual);
    }

    @Test
    public void testErrorQueueNameOverrideTakesPrecedenceOverErrorQueueNameExpression() {
        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
        consumerProperties.getExtension().setErrorQueueNameOverride("ErrorQueueNameOverride");
        consumerProperties.getExtension().setErrorQueueNameExpression("'AnErrorQueueNameExpression'");

        String actual = SolaceProvisioningUtil
                .getQueueNames("unused/destination", "unusedGroup", consumerProperties, false)
                .getErrorQueueName();
        assertEquals("ErrorQueueNameOverride", actual);
    }

    @Test
    public void testInvalidQueueNameExpression() {
        String invalidExpression = "This is an invalid expression";
        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
        consumerProperties.getExtension().setQueueNameExpression(invalidExpression);
        try {
            SolaceProvisioningUtil.getQueueNames("unused/destination", "unusedGroup", consumerProperties, false);
            fail("Expected expression evaluation to fail");
        } catch (Exception e) {
            Assertions.assertThat(e).isInstanceOf(ProvisioningException.class);
            Assertions.assertThat(e.getMessage()).contains("Failed to evaluate Spring expression: " + invalidExpression);
        }
    }

    @Test
    public void testQueueNameExpressionsForRequiredGroups() {
        String group1 = "group1_hasOverride";
        String group2 = "group2_noOverride";

        ExtendedProducerProperties<SolaceProducerProperties> producerProperties = new ExtendedProducerProperties<>(new SolaceProducerProperties());
        producerProperties.setRequiredGroups(group1, group2);
        producerProperties.getExtension().setQueueNameExpression("'DefaultQueueNameExpression'");

        Map<String, String> queueNameExpressionsForRequiredGroups = new HashMap<>();
        queueNameExpressionsForRequiredGroups.put(group1, "'ExpressionOverrideForGroup1'");
        producerProperties.getExtension().setQueueNameExpressionsForRequiredGroups(queueNameExpressionsForRequiredGroups);

        assertEquals("ExpressionOverrideForGroup1", SolaceProvisioningUtil.getQueueName("unused/destination", group1, producerProperties));
        assertEquals("DefaultQueueNameExpression", SolaceProvisioningUtil.getQueueName("unused/destination", group2, producerProperties));
    }
}
