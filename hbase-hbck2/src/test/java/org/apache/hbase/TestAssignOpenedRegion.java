package org.apache.hbase;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Hbck;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.rsgroup.RSGroupAdminClient;
import org.apache.hadoop.hbase.rsgroup.RSGroupAdminEndpoint;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hbase.rsgroup.RSGroupInfoManager.RSGROUP_TABLE_NAME;
import static org.junit.Assert.assertTrue;

/*
This test is among the integration tests, as I wanted to use the RS group admin and the
the tests in the hbase-server can not have dependency on hbase-rsgroup

we have two region servers, one handling the meta and the other handling an user table.
Using a custom coprocessor, we bring the user table to ABNORMALLY_CLOSED state
Assigning multiple times the same user region
 */
@Category(IntegrationTests.class)
public class TestAssignOpenedRegion {

  private static Logger LOG =
    LoggerFactory.getLogger(TestAssignOpenedRegion.class.getName());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static TableName TABLE_NAME = TableName.valueOf("testAssign");
  private static byte[] CF = Bytes.toBytes("cf");

  private static AtomicBoolean rsAbortNextTime = new AtomicBoolean(false);
  //private static AtomicReference<JVMClusterUtil.RegionServerThread> rsThreadToAbort;

  private HRegionServer metaRS;
  private HRegionServer userTableRS;
  private ServerName userTableRSName;
  private RSGroupAdminClient rsGroupAdmin;
  private Admin admin;


  @Before
  public void beforeMethod() throws Exception {
    TEST_UTIL.startMiniCluster(1);

  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }




  @Test
  public void reassignMultipleTimesAnAbnormallyClosedRegion() throws Exception {
    dumpMeta("beforeTest");

    // we create the test table
    // the region of the new table should land on userTableRS (as it is the only RS available that is not in "metaGroup")
    TEST_UTIL.createTable(TABLE_NAME, CF); // this will wait for the regions to be open
    dumpMeta("after table created");


    Admin admin = TEST_UTIL.getAdmin();
    String regionNameStr = admin.getRegions(TABLE_NAME).get(0).getEncodedName();
    HBCK2 hbck2 = new HBCK2(TEST_UTIL.getConfiguration());
    try (ClusterConnection connection = hbck2.connect(); Hbck hbck = connection.getHbck()) {
      List<Long> pids = hbck2.assigns(hbck, new String[]{regionNameStr});
      Thread.sleep(5000);
      dumpMeta("after assign using HBCK2");
    }

    List<RegionStateNode> regionsInTransition = TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates().getRegionsInTransition();
    assertTrue("ERROR! region found in transition: " + regionsInTransition.toString(),
               regionsInTransition.isEmpty());
  }

  private void dumpMeta(String comment) throws IOException {
    Table t = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    ResultScanner s = t.getScanner(new Scan());
    for (Result result : s) {
      RegionInfo info = HBCKMetaTableAccessor.getRegionInfo(result, HConstants.REGIONINFO_QUALIFIER);
      if (info == null) {
        continue;
      }
      LOG.info("META DUMP - {} regioninfo: {}", comment, Bytes.toStringBinary(result.getRow()) + info);

      byte[] stateBytes = result.getValue(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER);
      if (stateBytes == null) {
        continue;
      }
      LOG.info("META DUMP - {} state: {}", comment, Bytes.toStringBinary(result.getRow()) + Bytes.toString(stateBytes));

      byte[] serverBytes = result.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVERNAME_QUALIFIER);
      if (serverBytes == null) {
        continue;
      }
      LOG.info("META DUMP - {} sn: {}", comment, Bytes.toStringBinary(result.getRow()) + Bytes.toString(serverBytes));
    }
    s.close();
    t.close();
  }



}
