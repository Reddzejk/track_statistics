package statistics.testutils;

import io.flinkspector.datastream.DataStreamTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;


public class DataStreamTestBaseJUnit5 extends DataStreamTestBase {

    @BeforeEach
    private void initializeEnv() throws Exception {
        initialize();
    }

    @AfterEach
    private void runTest() throws Throwable {
        executeTest();
    }
}
