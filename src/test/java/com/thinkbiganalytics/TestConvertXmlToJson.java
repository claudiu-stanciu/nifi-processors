package com.thinkbiganalytics;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestConvertXmlToJson {

	final static Logger logger = LoggerFactory.getLogger(TestConvertXmlToJson.class);

	@Test
	public void testBundledXml() throws IOException {
		final Path XML_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-bundle.xml");
		final Path JSON_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-bundle.json");
		final TestRunner testRunner = TestRunners.newTestRunner(new ConvertXmlToJson());
		testRunner.setProperty(ConvertXmlToJson.PRETTY_PRINT_INDENT_FACTOR, "5");
		testRunner.enqueue(XML_SNIPPET);
		testRunner.run();
		testRunner.assertTransferCount(ConvertXmlToJson.REL_ORIGINAL, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_SUCCESS, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_FAILED, 0);
		final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ConvertXmlToJson.REL_SUCCESS).get(0);
		flowFile.assertContentEquals(JSON_SNIPPET);
	}

	@Test
	public void testWithEmptyXml() throws IOException {
		final Path XML_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-bundle.xml");
		final Path XML_SNIPPET_EMPTY = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-empty.xml");
		final TestRunner testRunner = TestRunners.newTestRunner(new ConvertXmlToJson());
		testRunner.enqueue(XML_SNIPPET);
		testRunner.enqueue(XML_SNIPPET_EMPTY);
		testRunner.run();
		testRunner.assertTransferCount(ConvertXmlToJson.REL_ORIGINAL, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_SUCCESS, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_FAILED, 0);
	}

	@Test
	public void testFailedParse() throws IOException {
		final Path XML_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-malformatted.xml");
		final TestRunner testRunner = TestRunners.newTestRunner(new ConvertXmlToJson());
		testRunner.setProperty(ConvertXmlToJson.PRETTY_PRINT_INDENT_FACTOR, "0");
		testRunner.enqueue(XML_SNIPPET);
		testRunner.run();
		testRunner.assertTransferCount(ConvertXmlToJson.REL_ORIGINAL, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_SUCCESS, 0);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_FAILED, 1);
	}

	@Test
	public void testWitsml() throws IOException {
		final Path XML_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-witsml.xml");
		final Path JSON_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-witsml.json");
		final TestRunner testRunner = TestRunners.newTestRunner(new ConvertXmlToJson());
		testRunner.setProperty(ConvertXmlToJson.PRETTY_PRINT_INDENT_FACTOR, "5");
		testRunner.enqueue(XML_SNIPPET);
		testRunner.run();
		testRunner.assertTransferCount(ConvertXmlToJson.REL_ORIGINAL, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_SUCCESS, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_FAILED, 0);
		final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ConvertXmlToJson.REL_SUCCESS).get(0);
		flowFile.assertContentEquals(JSON_SNIPPET);
	}
}
