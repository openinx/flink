/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tests.util;

import org.apache.flink.tests.util.categories.PreCommit;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.tests.util.kafka.KafkaResource;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.base.Charsets;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.StringUtils.byteToHexString;


@RunWith(Parameterized.class)
@Category(value = {TravisGroup1.class, PreCommit.class})
public class SQLClientKafkaITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(SQLClientKafkaITCase.class);

	private static final String KAFKA_JSON_SOURCE_SCHEMA_YAML = "kafka_json_source_schema.yaml";

	@Parameterized.Parameters(name = "{index}: kafka-version:{1} kafka-sql-version:{2} kafka-sql-jar-version:{3}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{"0.10.2.0", "0.10", "kafka-0.10"},
			{"0.11.0.2", "0.11", "kafka-0.11"},
			{"2.2.0", "universal", "kafka_"}
		});
	}

	@Rule
	public final KafkaResource kafka;

	@Rule
	public final FlinkResource flink = FlinkResource.get();

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final String kafkaSQLVersion;
	private final Path result;
	private final Path sqlClientSessionConf;

	private final Path sqlAvroJar;

	private final Path sqlJsonJar;
	private final Path sqlToolBoxJar;
	private final Path sqlConnectorKafkaJar;

	public SQLClientKafkaITCase(String kafkaVersion, String kafkaSQLVersion, String kafkaSQLJarPattern) throws IOException {
		this.kafka = KafkaResource.get(kafkaVersion);
		this.kafkaSQLVersion = kafkaSQLVersion;

		tmp.create();
		Path tmpPath = tmp.getRoot().toPath();
		LOG.info("The current temporary path: {}", tmpPath);
		this.sqlClientSessionConf = tmpPath.resolve("sql-client-session.conf");
		this.result = tmpPath.resolve("result");

		this.sqlAvroJar = TestUtils.getResourceJar("flink-sql-client-test.*sql-jars.*avro");
		this.sqlJsonJar = TestUtils.getResourceJar("flink-sql-client-test.*sql-jars.*json");
		this.sqlToolBoxJar = TestUtils.getResourceJar("flink-sql-client-test.*/SqlToolbox.jar");
		this.sqlConnectorKafkaJar = TestUtils.getResourceJar("flink-sql-client-test.*sql-jars.*" + kafkaSQLJarPattern);
	}

	@Test
	public void testKafka() throws Exception {
		try (ClusterController clusterController = flink.startCluster(2)) {
			// Create topic and send message
			String testJsonTopic = "test-json";
			String testAvroTopic = "test-avro";
			kafka.createTopic(1, 1, testJsonTopic);
			String[] messages = new String[]{
				"'{\"timestamp\": \"2018-03-12T08:00:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}'",
				"'{\"timestamp\": \"2018-03-12T08:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is a warning.\"}}'",
				"'{\"timestamp\": \"2018-03-12T09:00:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"WARNING\", \"message\": \"This is another warning.\"}}'",
				"'{\"timestamp\": \"2018-03-12T09:10:00Z\", \"user\": \"Alice\", \"event\": { \"type\": \"INFO\", \"message\": \"This is a info.\"}}'",
				"'{\"timestamp\": \"2018-03-12T09:20:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}'",
				"'{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": \"Steve\", \"event\": { \"type\": \"INFO\", \"message\": \"This is another info.\"}}'",
				"'{\"timestamp\": \"2018-03-12T09:30:00Z\", \"user\": null, \"event\": { \"type\": \"WARNING\", \"message\": \"This is a bad message because the user is missing.\"}}'",
				"'{\"timestamp\": \"2018-03-12T10:40:00Z\", \"user\": \"Bob\", \"event\": { \"type\": \"ERROR\", \"message\": \"This is an error.\"}}'"
			};
			kafka.sendMessages(testJsonTopic, messages);

			// Create topic test-avro
			kafka.createTopic(1, 1, testAvroTopic);

			// Initialize the SQL client session configuration file
			Map<String, String> varsMap = new HashMap<>();
			varsMap.put("$TABLE_NAME", "JsonSourceTable");
			varsMap.put("$KAFKA_SQL_VERSION", this.kafkaSQLVersion);
			varsMap.put("$TOPIC_NAME", testJsonTopic);
			varsMap.put("$RESULT", this.result.toAbsolutePath().toString());
			String schemaContent = initializeSessionYaml(varsMap);
			Files.write(this.sqlClientSessionConf,
				schemaContent.getBytes(Charsets.UTF_8),
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE);

			{
				LOG.info("Executing SQL: Kafka {} JSON -> Kafka {} Avro", kafkaSQLVersion, kafkaSQLVersion);
				String sqlStatement1 = "INSERT INTO AvroBothTable\n" +
					"  SELECT\n" +
					"    CAST(TUMBLE_START(rowtime, INTERVAL '1' HOUR) AS VARCHAR) AS event_timestamp,\n" +
					"    user,\n" +
					"    RegReplace(event.message, ' is ', ' was ') AS message,\n" +
					"    COUNT(*) AS duplicate_count\n" +
					"  FROM JsonSourceTable\n" +
					"  WHERE user IS NOT NULL\n" +
					"  GROUP BY\n" +
					"    user,\n" +
					"    event.message,\n" +
					"    TUMBLE(rowtime, INTERVAL '1' HOUR)";

				clusterController.submitSQLJob(new SQLJobSubmission.SQLJobSubmissionBuilder()
					.isEmbedded(true)
					.addJar(sqlAvroJar)
					.addJar(sqlJsonJar)
					.addJar(sqlConnectorKafkaJar)
					.addJar(sqlToolBoxJar)
					.addJar(Paths.get("/Users/openinx/software/flink/build-target/opt/flink-oss-fs-hadoop-1.10-SNAPSHOT.jar"))
					.setSessionEnvFile(this.sqlClientSessionConf.toAbsolutePath().toString())
					.addSQL(sqlStatement1)
					.build());
			}

			{
				LOG.info("Executing SQL: Kafka {} Avro -> Csv sink", kafkaSQLVersion);
				String sqlStatement2 = "INSERT INTO CsvSinkTable\n" +
					"   SELECT AvroBothTable.*, RegReplace('Test constant folding.', 'Test', 'Success') AS constant\n" +
					"   FROM AvroBothTable";

				clusterController.submitSQLJob(new SQLJobSubmission.SQLJobSubmissionBuilder()
					.isEmbedded(true)
					.addJar(sqlAvroJar)
					.addJar(sqlJsonJar)
					.addJar(sqlConnectorKafkaJar)
					.addJar(sqlToolBoxJar)
					.addJar(Paths.get("/Users/openinx/software/flink/build-target/opt/flink-oss-fs-hadoop-1.10-SNAPSHOT.jar"))
					.setSessionEnvFile(this.sqlClientSessionConf.toAbsolutePath().toString())
					.addSQL(sqlStatement2)
					.build()
				);
			}

			// Wait until all the results flushed to the CSV file.
			LOG.info("Verify the CSV result.");
			checkCsvResultFile();
			LOG.info("The Kafka({}) SQL client test run successfully.", this.kafkaSQLVersion);
		}
	}

	private String initializeSessionYaml(Map<String, String> vars) throws IOException {
		URL url = SQLClientKafkaITCase.class.getClassLoader().getResource(KAFKA_JSON_SOURCE_SCHEMA_YAML);
		if (url == null) {
			throw new FileNotFoundException(KAFKA_JSON_SOURCE_SCHEMA_YAML);
		}

		String schema = FileUtils.readFileUtf8(new File(url.getFile()));
		for (Map.Entry<String, String> var : vars.entrySet()) {
			schema = schema.replace(var.getKey(), var.getValue());
		}
		return schema;
	}

	private void checkCsvResultFile() throws Exception {
		boolean success = false;
		long maxRetries = 10, duration = 5000L;
		for (int i = 0; i < maxRetries; i++) {
			if (Files.exists(result)) {
				List<String> lines = Files.readAllLines(result);
				if (lines.size() == 4) {
					success = true;
					// Check the MD5SUM of the result file.
					Assert.assertEquals("MD5 checksum mismatch", "390b2985cbb001fbf4d301980da0e7f0", getMd5Sum(result));
					break;
				}
			} else {
				LOG.info("The target CSV {} does not exist now", result);
			}
			Thread.sleep(duration);
		}
		Assert.assertTrue("Timeout(" + (maxRetries * duration) + " sec) to read the correct CSV results.", success);
	}

	private static String getMd5Sum(Path path) throws Exception {
		MessageDigest md = MessageDigest.getInstance("MD5");
		try (InputStream is = Files.newInputStream(path)) {
			DigestInputStream dis = new DigestInputStream(is, md);
			byte[] buf = new byte[1024];
			for (; dis.read(buf) > 0; ) {
			}
		}
		return byteToHexString(md.digest());
	}
}
