
import eu.dissco.doec.digitalObjectRepository.DigitalObjectRepositoryClient;
import eu.dissco.doec.digitalObjectRepository.DigitalObjectRepositoryException;
import eu.dissco.doec.digitalObjectRepository.DigitalObjectRepositoryInfo;
import eu.dissco.doec.utils.FileUtils;
import net.dona.doip.client.DigitalObject;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.*;

@Ignore
public class DigitalObjectRepositoryClientTest {

    private final static Logger logger = LoggerFactory.getLogger(DigitalObjectRepositoryClientTest.class);

    private static DigitalObjectRepositoryClient digitalObjectRepositoryClient;
    private static DigitalObjectRepositoryClient provenanceRepositoryClient;

    @BeforeClass
    public static void setup() throws ConfigurationException, DigitalObjectRepositoryException {
        Configuration config = FileUtils.loadConfigurationFromResourceFile("config.properties");
        DigitalObjectRepositoryInfo digitalObjectRepositoryInfo =  DigitalObjectRepositoryInfo.getDigitalObjectRepositoryInfoFromConfig(config);
        digitalObjectRepositoryClient = new DigitalObjectRepositoryClient(digitalObjectRepositoryInfo);

        DigitalObjectRepositoryInfo provenanceRepositoryInfo =  DigitalObjectRepositoryInfo.getProvenanceRepositoryInfoFromConfig(config);
        provenanceRepositoryClient = new DigitalObjectRepositoryClient(provenanceRepositoryInfo);
    }

    @AfterClass
    public static void tearDown() {
        digitalObjectRepositoryClient.close();
        provenanceRepositoryClient.close();
    }

    @Test
    public void testDoipServerIsUp() throws DigitalObjectRepositoryException {
        DigitalObject helloResponse = digitalObjectRepositoryClient.hello();
        assertNotNull("Hello response shouldn't be null", helloResponse);

        Double protocolVersion = helloResponse.attributes.get("protocolVersion").getAsDouble();
        assertTrue("Protocol should be >=2.0",protocolVersion>=2.0);
    }


}