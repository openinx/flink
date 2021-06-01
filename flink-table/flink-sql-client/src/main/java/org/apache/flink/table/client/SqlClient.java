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

package org.apache.flink.table.client;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.local.LocalExecutor;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.client.config.entries.ConfigurationEntry.create;
import static org.apache.flink.table.client.config.entries.ConfigurationEntry.merge;

/**
 * SQL Client for submitting SQL statements. The client can be executed in two modes: a gateway and
 * embedded mode.
 *
 * <p>- In embedded mode, the SQL CLI is tightly coupled with the executor in a common process. This
 * allows for submitting jobs without having to start an additional component.
 *
 * <p>- In future versions: In gateway mode, the SQL CLI client connects to the REST API of the
 * gateway and allows for managing queries via console.
 *
 * <p>For debugging in an IDE you can execute the main method of this class using: "embedded
 * --defaults /path/to/sql-client-defaults.yaml --jar /path/to/target/flink-sql-client-*.jar"
 *
 * <p>Make sure that the FLINK_CONF_DIR environment variable is set.
 */
public class SqlClient {

    private static final Logger LOG = LoggerFactory.getLogger(SqlClient.class);

    private final boolean isEmbedded;
    private final CliOptions options;

    public static final String MODE_EMBEDDED = "embedded";
    public static final String MODE_GATEWAY = "gateway";

    public static final String DEFAULT_SESSION_ID = "default";

    public SqlClient(boolean isEmbedded, CliOptions options) {
        this.isEmbedded = isEmbedded;
        this.options = options;
    }

    private void start() {
        if (isEmbedded) {
            // create local executor with default environment
            final List<URL> jars;
            if (options.getJars() != null) {
                jars = options.getJars();
            } else {
                jars = Collections.emptyList();
            }
            final List<URL> libDirs;
            if (options.getLibraryDirs() != null) {
                libDirs = options.getLibraryDirs();
            } else {
                libDirs = Collections.emptyList();
            }
            final Executor executor = new LocalExecutor(options.getDefaults(), jars, libDirs);
            executor.start();

            // create CLI client with session environment
            final Environment sessionEnv = readSessionEnvironment(options.getEnvironment());
            appendPythonConfig(sessionEnv, options.getPythonConfiguration());
            final SessionContext context;
            if (options.getSessionId() == null) {
                context = new SessionContext(DEFAULT_SESSION_ID, sessionEnv);
            } else {
                context = new SessionContext(options.getSessionId(), sessionEnv);
            }

            // Open an new session
            String sessionId = executor.openSession(context);
            try {
                // add shutdown hook
                Runtime.getRuntime()
                        .addShutdownHook(new EmbeddedShutdownThread(sessionId, executor));

                // do the actual work
                openCli(sessionId, executor);
            } finally {
                executor.closeSession(sessionId);
            }
        } else {
            throw new SqlClientException("Gateway mode is not supported yet.");
        }
    }

    /**
     * Opens the CLI client for executing SQL statements.
     *
     * @param sessionId session identifier for the current client.
     * @param executor executor
     */
    private void openCli(String sessionId, Executor executor) {
        Path historyFilePath;
        if (options.getHistoryFilePath() != null) {
            historyFilePath = Paths.get(options.getHistoryFilePath());
        } else {
            historyFilePath =
                    Paths.get(
                            System.getProperty("user.home"),
                            SystemUtils.IS_OS_WINDOWS ? "flink-sql-history" : ".flink-sql-history");
        }

        boolean hasSqlFile = options.getSqlFile() != null;
        boolean hasUpdateStatement = options.getUpdateStatement() != null;
        if (hasSqlFile && hasUpdateStatement) {
            throw new IllegalArgumentException(
                    String.format(
                            "Please use either option %s or %s. The option %s is deprecated and it's suggested to use %s instead.",
                            CliOptionsParser.OPTION_FILE,
                            CliOptionsParser.OPTION_UPDATE,
                            CliOptionsParser.OPTION_UPDATE.getOpt(),
                            CliOptionsParser.OPTION_FILE.getOpt()));
        }

        try (CliClient cli = new CliClient(sessionId, executor, historyFilePath)) {
            if (hasSqlFile) {
                cli.executeFile(readFromURL(options.getSqlFile()));
            } else if (options.getUpdateStatement() == null) {
                // interactive CLI mode
                cli.open();
            } else {
                // execute single update statement
                final boolean success = cli.submitUpdate(options.getUpdateStatement());
                if (!success) {
                    throw new SqlClientException(
                            "Could not submit given SQL update statement to cluster.");
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    private static Environment readSessionEnvironment(URL envUrl) {
        // use an empty environment by default
        if (envUrl == null) {
            System.out.println("No session environment specified.");
            return new Environment();
        }

        System.out.println("Reading session environment from: " + envUrl);
        LOG.info("Using session environment file: {}", envUrl);
        try {
            return Environment.parse(envUrl);
        } catch (IOException e) {
            throw new SqlClientException(
                    "Could not read session environment file at: " + envUrl, e);
        }
    }

    private static void appendPythonConfig(Environment env, Configuration pythonConfiguration) {
        Map<String, Object> pythonConfig = new HashMap<>(pythonConfiguration.toMap());
        Map<String, Object> combinedConfig =
                new HashMap<>(merge(env.getConfiguration(), create(pythonConfig)).asMap());
        env.setConfiguration(combinedConfig);
    }

    // --------------------------------------------------------------------------------------------

    public static void main(String[] args) {
        if (args.length < 1) {
            CliOptionsParser.printHelpClient();
            return;
        }

        switch (args[0]) {
            case MODE_EMBEDDED:
                // remove mode
                final String[] modeArgs = Arrays.copyOfRange(args, 1, args.length);
                final CliOptions options = CliOptionsParser.parseEmbeddedModeClient(modeArgs);
                if (options.isPrintHelp()) {
                    CliOptionsParser.printHelpEmbeddedModeClient();
                } else {
                    try {
                        final SqlClient client = new SqlClient(true, options);
                        client.start();
                    } catch (SqlClientException e) {
                        // make space in terminal
                        System.out.println();
                        System.out.println();
                        LOG.error("SQL Client must stop.", e);
                        throw e;
                    } catch (Throwable t) {
                        // make space in terminal
                        System.out.println();
                        System.out.println();
                        LOG.error(
                                "SQL Client must stop. Unexpected exception. This is a bug. Please consider filing an issue.",
                                t);
                        throw new SqlClientException(
                                "Unexpected exception. This is a bug. Please consider filing an issue.",
                                t);
                    }
                }
                break;

            case MODE_GATEWAY:
                throw new SqlClientException("Gateway mode is not supported yet.");

            default:
                CliOptionsParser.printHelpClient();
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class EmbeddedShutdownThread extends Thread {

        private final String sessionId;
        private final Executor executor;

        public EmbeddedShutdownThread(String sessionId, Executor executor) {
            this.sessionId = sessionId;
            this.executor = executor;
        }

        @Override
        public void run() {
            // Shutdown the executor
            System.out.println("\nShutting down the session...");
            executor.closeSession(sessionId);
            System.out.println("done.");
        }
    }

    private String readFromURL(URL file) {
        try {
            return IOUtils.toString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new SqlExecutionException(
                    String.format("Fail to read content from the %s.", file.getPath()), e);
        }
    }
}
