package cascading.hbase;

import java.io.IOException;
import java.util.Map;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.property.AppProps;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;

abstract public class HBaseTests
  {

  /** The configuration. */
  protected static Configuration configuration;

  private static HBaseTestingUtility utility;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception
    {
    System.setProperty( HBaseCommonTestingUtility.BASE_TEST_DIRECTORY_KEY, "build/test-data" );
    utility = new HBaseTestingUtility();
    utility.startMiniCluster( 1 );
    configuration = utility.getConfiguration();
    }

  protected static void deleteTable( Configuration configuration, String tableName ) throws IOException
    {
    HBaseAdmin hbase = new HBaseAdmin( configuration );
    if( hbase.tableExists( Bytes.toBytes( tableName ) ) )
      {
      hbase.disableTable( Bytes.toBytes( tableName ) );
      hbase.deleteTable( Bytes.toBytes( tableName ) );
      }
    hbase.close();
    }

  @AfterClass
  public static void tearDownAfterClass() throws Exception
    {
    utility.shutdownMiniCluster();
    }

  public FlowConnector createHadoopFlowConnector()
    {
    return createHadoopFlowConnector( Maps.newHashMap() );
    }

  public FlowConnector createHadoopFlowConnector( Map<Object, Object> props )
    {
    Map<Object, Object> finalProperties = Maps.newHashMap( props );
    finalProperties.put( HConstants.ZOOKEEPER_CLIENT_PORT, utility.getZkCluster().getClientPort() );
    AppProps.setApplicationName( finalProperties, getClass().getName() );
    return new HadoopFlowConnector( finalProperties );
    }

  }
