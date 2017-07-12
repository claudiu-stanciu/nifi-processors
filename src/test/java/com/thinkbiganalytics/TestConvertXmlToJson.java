package com.thinkbiganalytics;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestConvertXmlToJson {

	final static Logger logger = LoggerFactory.getLogger(TestConvertXmlToJson.class);

	@Test
	public void testParseSmallXml() throws IOException {
		final Path XML_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-small.xml");
		final Path JSON_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-small.json");
		final TestRunner testRunner = TestRunners.newTestRunner(new ConvertXmlToJson());
		testRunner.setProperty(ConvertXmlToJson.PROP_PP_INDENT_FACTOR, "5");
		testRunner.enqueue(XML_SNIPPET);
		testRunner.run();
		testRunner.assertTransferCount(ConvertXmlToJson.REL_ORIGINAL, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_SUCCESS, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_FAILED, 0);
		final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ConvertXmlToJson.REL_SUCCESS).get(0);
		flowFile.assertContentEquals(JSON_SNIPPET);
	}

	@Test
	public void testFailedParse() throws IOException {
		final Path XML_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-malformatted.xml");
		final TestRunner testRunner = TestRunners.newTestRunner(new ConvertXmlToJson());
		testRunner.setProperty(ConvertXmlToJson.PROP_PP_INDENT_FACTOR, "0");
		testRunner.enqueue(XML_SNIPPET);
		testRunner.run();
		testRunner.assertTransferCount(ConvertXmlToJson.REL_ORIGINAL, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_SUCCESS, 0);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_FAILED, 1);
	}

	@Test
	public void testBundledXml() throws IOException {
		final Path XML_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-bundle.xml");
		final Path JSON_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-bundle.json");
		final TestRunner testRunner = TestRunners.newTestRunner(new ConvertXmlToJson());
		testRunner.setProperty(ConvertXmlToJson.PROP_PP_INDENT_FACTOR, "5");
		testRunner.enqueue(XML_SNIPPET);
		testRunner.run();
		testRunner.assertTransferCount(ConvertXmlToJson.REL_ORIGINAL, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_SUCCESS, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_FAILED, 0);
		final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ConvertXmlToJson.REL_SUCCESS).get(0);
		flowFile.assertContentEquals(JSON_SNIPPET);
	}

	@Test
	public void testWitsml() throws IOException {
		final Path XML_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-witsml.xml");
		final Path JSON_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-witsml.json");
		final TestRunner testRunner = TestRunners.newTestRunner(new ConvertXmlToJson());
		testRunner.setProperty(ConvertXmlToJson.PROP_PP_INDENT_FACTOR, "5");
		testRunner.enqueue(XML_SNIPPET);
		testRunner.run();
		testRunner.assertTransferCount(ConvertXmlToJson.REL_ORIGINAL, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_SUCCESS, 1);
		testRunner.assertTransferCount(ConvertXmlToJson.REL_FAILED, 0);
		final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ConvertXmlToJson.REL_SUCCESS).get(0);
		flowFile.assertContentEquals(JSON_SNIPPET);
	}

	@Test
	public void testInvalidXml() {
		final Path XML_SNIPPET = Paths.get("src/test/resources/TestConvertXmlToJson/snippet-invalid.xml");
		try {
			String content = new String(Files.readAllBytes(XML_SNIPPET));
			//ConvertXmlToJson convert = new ConvertXmlToJson();
			//assertTrue();
			boolean valid = ConvertXmlToJson.validXml(content);
			assertEquals(false, valid);
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
