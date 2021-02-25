package org.apache.hbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
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
public class TestRepeatedAssign {

  private static Logger LOG =
    LoggerFactory.getLogger(TestRepeatedAssign.class.getName());

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
    LOG.info("Setting up TestRepeatedAssign");
    TEST_UTIL.getConfiguration().set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, RSGroupBasedLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, RSGroupAdminEndpoint.class.getName());
    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
                                            CrashSecondRSAfterOpening.class.getName());
    TEST_UTIL.startMiniCluster(1);
    metaRS = TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(0).getRegionServer();

    // creating a new region server
    TEST_UTIL.getMiniHBaseCluster().startRegionServer();
    TEST_UTIL.waitFor(15000, () -> TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() == 2);
    userTableRS = TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(1).getRegionServer();
    userTableRSName = userTableRS.getServerName();

    rsGroupAdmin = new RSGroupAdminClient(TEST_UTIL.getConnection());

    rsGroupAdmin.addRSGroup("metaGroup");
    rsGroupAdmin.moveServers(Sets.newHashSet(metaRS.getServerName().getAddress()), "metaGroup");
    rsGroupAdmin.moveTables((Sets.newHashSet(TableName.META_TABLE_NAME, TableName.NAMESPACE_TABLE_NAME, RSGROUP_TABLE_NAME)), "metaGroup");

    TEST_UTIL.waitFor(30000, ()-> {
      if(!metaRS.getRegions().stream().anyMatch((r)->r.getTableDescriptor().getTableName().equals(TableName.META_TABLE_NAME))) {
        LOG.info("meta table is on wrong server, let's wait...");
        return false;
      }
      if(!metaRS.getRegions().stream().anyMatch((r)->r.getTableDescriptor().getTableName().equals(TableName.NAMESPACE_TABLE_NAME))) {
        LOG.info("namespace table is on wrong server, let's wait...");
        return false;
      }
      if(!metaRS.getRegions().stream().anyMatch((r)->r.getTableDescriptor().getTableName().equals(RSGROUP_TABLE_NAME))) {
        LOG.info("rsgroup table is on wrong server, let's wait...");
        return false;
      }
      return true;
    });

    admin = TEST_UTIL.getAdmin();

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

    TEST_UTIL.getMiniHBaseCluster().stopRegionServer(1);
    TEST_UTIL.waitFor(15000, () -> TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() == 1);
    Thread.sleep(5000); // make sure the stopped RS totally dead and all threads exited
    dumpMeta("after RS stopped");

    // starting the RS again, but this time the coprocessor will abort the region server
    rsAbortNextTime.set(true);
    TEST_UTIL.getMiniHBaseCluster().startRegionServer(userTableRSName.getHostname(), userTableRSName.getPort());


    Thread.sleep(5000); // TODO: wait until ABNORMALLY_CLOSED
    TEST_UTIL.waitFor(15000, () -> TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size() == 1);
    Thread.sleep(5000);
    LOG.info("coprocessor aborted the RS");
    dumpMeta("after RS abortion");

    // restart the second RS again
    // (no abortion this time, as rsAbortNextTime was set to false by the coprocessor
    TEST_UTIL.getMiniHBaseCluster().startRegionServer(userTableRSName.getHostname(), userTableRSName.getPort());

    // we expect now, that the user table got assigned and opened on the second RS
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    dumpMeta("after RS restart");

    // now we make a manual assign
/*    byte[] regionName = admin.getRegions(TABLE_NAME).get(0).getRegionName();
    try {
      admin.assign(regionName);
    } catch (DoNotRetryRegionException e) {
      // this is expected
      assertTrue(e.getMessage().startsWith("Unexpected state for rit=OPEN"));
    }
    dumpMeta("after assign on Admin API");
*/
    //however! if we do the assignment using HBCK2 (which will call hbck.assign(),
    // then this check will be skipped, so we will re-assign an already assigned region
    // this will be swallowed by the RS, never answering to the Master, who
    // will notice this as a stucked assignment and keeping OPENING state in the meta

    HBCK2 hbck2 = new HBCK2(TEST_UTIL.getConfiguration());
    try (ClusterConnection connection = hbck2.connect(); Hbck hbck = connection.getHbck()) {
      String regionNameStr = admin.getRegions(TABLE_NAME).get(0).getEncodedName();
      List<Long> pids = hbck2.assigns(hbck, new String[]{regionNameStr});
      Thread.sleep(5000);
      dumpMeta("after assign using HBCK2");

    }

    try (ClusterConnection connection = hbck2.connect(); Hbck hbck = connection.getHbck()) {
      String regionNameStr = admin.getRegions(TABLE_NAME).get(0).getEncodedName();
      List<Long> pids = hbck2.assigns(hbck, new String[]{regionNameStr});
      Thread.sleep(5000);
      dumpMeta("after assign using HBCK2");

    }

  }



  private void dumpMeta(String comment) throws IOException {
    Table t = TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    ResultScanner s = t.getScanner(new Scan());
    for (Result result : s) {
      RegionInfo info = MetaTableAccessor.getRegionInfo(result);
      if (info == null) {
        continue;
      }
      LOG.info("META DUMP - {} regioninfo: {}", comment, Bytes.toStringBinary(result.getRow()) + info);

      byte[] stateBytes = result.getValue(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER);
      if (stateBytes == null) {
        continue;
      }
      LOG.info("META DUMP - {} state: {}", comment, Bytes.toStringBinary(result.getRow()) + Bytes.toString(stateBytes));

      byte[] serverBytes = result.getValue(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
      if (serverBytes == null) {
        continue;
      }
      LOG.info("META DUMP - {} server: {}", comment, Bytes.toStringBinary(result.getRow()) + Bytes.toString(serverBytes));
    }
    s.close();
    t.close();
  }



  public static class CrashSecondRSAfterOpening implements RegionCoprocessor, RegionObserver {

    static AtomicInteger regionOpenAttempts = new AtomicInteger(0);

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
      if (c.getEnvironment().getRegion().getRegionInfo().getTable().equals(TABLE_NAME)) {

        int openAttempts = regionOpenAttempts.incrementAndGet();
        LOG.info("CrashSecondRSAfterOpening: region opening attempt {}", openAttempts);

        if(rsAbortNextTime.getAndSet(false)) {
          TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().get(1).getRegionServer().abort("CrashSecondRSAfterOpening: aborting RS");
          LOG.info("CrashSecondRSAfterOpening: RS shutdown called");
        }
      }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }
  }

}
