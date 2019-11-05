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

/**
 * End-to-end test for Kafka 0.11 version.
 */
public class TestStreamingKafka011 extends TestStreamingKafka {

	@Override
	protected void prepareKafkaEnv() {
		this.kafkaResource = KafkaResourceFactory.create(
			"https://mirrors.tuna.tsinghua.edu.cn/apache/kafka/2.1.1/kafka_2.11-2.1.1.tgz",
			"kafka_2.11-2.1.1.tgz",
			End2EndUtil.getTestDataDir()
		);
		this.jarFile = End2EndUtil
			.getEnd2EndModuleDir()
			.resolve("flink-streaming-kafka011-test/target/Kafka011Example.jar");
	}
}
