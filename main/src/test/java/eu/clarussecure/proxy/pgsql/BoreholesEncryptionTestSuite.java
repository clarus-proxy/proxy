package eu.clarussecure.proxy.pgsql;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ BoreholesSimpleEncryptionIT.class, BoreholesSearchableEncryptionIT.class })
public class BoreholesEncryptionTestSuite {
}
