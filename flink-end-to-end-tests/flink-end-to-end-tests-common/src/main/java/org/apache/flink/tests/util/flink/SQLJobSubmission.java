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

package org.apache.flink.tests.util.flink;

import org.apache.flink.util.Preconditions;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Programmatic definition of a SQL job-submission.
 */
public class SQLJobSubmission {

	private boolean embedded;
	private String defaultEnvFile;
	private String sessionEnvFile;
	private List<String> jars;
	private String sql;

	private SQLJobSubmission(
		boolean embedded,
		String defaultEnvFile,
		String sessionEnvFile,
		List<String> jars,
		String sql
	) {
		Preconditions.checkNotNull(jars);
		Preconditions.checkNotNull(sql);

		this.embedded = embedded;
		this.defaultEnvFile = defaultEnvFile;
		this.sessionEnvFile = sessionEnvFile;
		this.jars = jars;
		this.sql = sql;
	}

	public boolean isEmbedded() {
		return embedded;
	}

	public String getDefaultEnvFile() {
		return defaultEnvFile;
	}

	public String getSessionEnvFile() {
		return sessionEnvFile;
	}

	public List<String> getJars() {
		return this.jars;
	}

	public String getSQL(){
		return this.sql;
	}

	/**
	 * Builder for the {@link SQLJobSubmission}.
	 */
	public static class SQLJobSubmissionBuilder {
		private List<String> jars = new ArrayList<>();
		private boolean embedded = true;
		private String defaultEnvFile = null;
		private String sessionEnvFile = null;
		private String sql = null;

		public SQLJobSubmissionBuilder isEmbedded(boolean embedded) {
			this.embedded = embedded;
			return this;
		}

		public SQLJobSubmissionBuilder setDefaultEnvFile(String defaultEnvFile) {
			this.defaultEnvFile = defaultEnvFile;
			return this;
		}

		public SQLJobSubmissionBuilder setSessionEnvFile(String sessionEnvFile) {
			this.sessionEnvFile = sessionEnvFile;
			return this;
		}

		public SQLJobSubmissionBuilder addJar(Path jarFile) {
			this.jars.add(jarFile.toAbsolutePath().toString());
			return this;
		}

		public SQLJobSubmissionBuilder addSQL(String sql) {
			this.sql = sql;
			return this;
		}

		public SQLJobSubmission build() {
			return new SQLJobSubmission(embedded, defaultEnvFile, sessionEnvFile, jars, sql);
		}
	}
}
