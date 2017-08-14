package eu.clarussecure.proxy.pgsql;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ MicroaggregationForCoarseningBoreholesIT.class, ShiftingForCoarseningBoreholesIT.class,
        SplittingBoreholesIT.class })
public class ProtectingBoreholesTestSuite {
}
