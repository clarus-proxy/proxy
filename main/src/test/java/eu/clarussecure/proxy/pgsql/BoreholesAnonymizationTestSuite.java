package eu.clarussecure.proxy.pgsql;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ CoarseningBoreholesByMicroaggregationIT.class, CoarseningBoreholesByShiftingIT.class })
public class BoreholesAnonymizationTestSuite {
}
