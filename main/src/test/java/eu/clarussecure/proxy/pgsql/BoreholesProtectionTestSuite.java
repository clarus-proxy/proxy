package eu.clarussecure.proxy.pgsql;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ BoreholesAnonymizationTestSuite.class, BoreholesSplittingTestSuite.class,
        BoreholesEncryptionTestSuite.class })
public class BoreholesProtectionTestSuite {
}
