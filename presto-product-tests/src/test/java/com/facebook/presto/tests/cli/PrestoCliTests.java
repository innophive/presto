/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.tests.cli;

import com.facebook.presto.cli.Presto;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.teradata.test.AfterTestWithContext;
import com.teradata.test.ProductTest;
import com.teradata.test.Requirement;
import com.teradata.test.RequirementsProvider;
import com.teradata.test.fulfillment.table.ImmutableTableRequirement;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.facebook.presto.tests.TestGroups.CLI;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.readLines;
import static com.teradata.test.fulfillment.hive.tpch.TpchTableDefinitions.NATION;
import static com.teradata.test.process.CliProcess.trimLines;
import static com.teradata.test.process.JavaProcessLauncher.defaultJavaProcessLauncher;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class PrestoCliTests
        extends ProductTest
        implements RequirementsProvider
{
    private static final long TIMEOUT = 300 * 1000; // 30 secs per test
    private static final String EXIT_COMMAND = "exit";

    private final List<String> nationTableInteractiveLines;
    private final List<String> nationTableBatchLines;

    @Inject
    @Named("databases.presto.server_address")
    private String serverAddress;

    private PrestoCliProcess presto;

    public PrestoCliTests()
            throws IOException
    {
        nationTableInteractiveLines = readLines(getResource("com/facebook/presto/tests/cli/interactive_query.results"), UTF_8);
        nationTableBatchLines = readLines(getResource("com/facebook/presto/tests/cli/batch_query.results"), UTF_8);
    }

    @AfterTestWithContext
    public void stopPresto()
            throws InterruptedException
    {
        if (presto != null) {
            presto.getInput().println(EXIT_COMMAND);
            presto.waitWithTimeoutAndKill();
        }
    }

    @Override
    public Requirement getRequirements()
    {
        return new ImmutableTableRequirement(NATION);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldDisplayVersion()
            throws IOException, InterruptedException
    {
        launchPrestoCli("--version");
        String version = firstNonNull(Presto.class.getPackage().getImplementationVersion(), "(version unknown)");
        assertThat(presto.readRemainingLines()).containsExactly("Presto CLI " + version);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunQuery()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument();
        presto.waitForPrompt();
        presto.getInput().println("select * from hive.default.nation;");
        assertThat(trimLines(presto.readLinesUntilPrompt())).containsAll(nationTableInteractiveLines);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunBatchQuery()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("--execute", "select * from hive.default.nation;");
        assertThat(trimLines(presto.readRemainingLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldUseCatalogAndSchemaOptions()
            throws IOException, InterruptedException
    {
        launchPrestoCliWithServerArgument("--catalog", "hive", "--schema", "default", "--execute", "select * from nation;");
        assertThat(trimLines(presto.readRemainingLines())).containsAll(nationTableBatchLines);
    }

    @Test(groups = CLI, timeOut = TIMEOUT)
    public void shouldRunQueryFromFile()
            throws IOException, InterruptedException
    {
        File temporayFile = File.createTempFile("test-sql", null);
        temporayFile.deleteOnExit();
        Files.write("select * from hive.default.nation;\n", temporayFile, UTF_8);

        launchPrestoCliWithServerArgument("--file", temporayFile.getAbsolutePath());
        assertThat(trimLines(presto.readRemainingLines())).containsAll(nationTableBatchLines);
    }

    private void launchPrestoCliWithServerArgument(String... arguments)
            throws IOException, InterruptedException
    {
        launchPrestoCli(ImmutableList.<String>builder().add("--server", serverAddress).add(arguments).build());
    }

    private void launchPrestoCli(String... arguments)
            throws IOException, InterruptedException
    {
        launchPrestoCli(asList(arguments));
    }

    private void launchPrestoCli(List<String> arguments)
            throws IOException, InterruptedException
    {
        presto = new PrestoCliProcess(defaultJavaProcessLauncher().launch(Presto.class, arguments));
    }
}
