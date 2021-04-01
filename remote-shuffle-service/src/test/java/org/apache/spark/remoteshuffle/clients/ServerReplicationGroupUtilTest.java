/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.clients;

import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.common.ServerReplicationGroup;
import org.apache.spark.remoteshuffle.exceptions.RssInvalidStateException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ServerReplicationGroupUtilTest {

  @Test
  public void createReplicationGroups_oneReplica() {
    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil
        .createReplicationGroups(Arrays.asList(new ServerDetail("server1", "host:9000")), 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    Assert.assertEquals(replicationGroups.get(0).getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000")));

    replicationGroups = ServerReplicationGroupUtil.createReplicationGroups(Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")),
        1);
    Assert.assertEquals(replicationGroups.size(), 2);
    Assert.assertEquals(replicationGroups.get(0).getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000")));
    Assert.assertEquals(replicationGroups.get(1).getServers(),
        Arrays.asList(new ServerDetail("server2", "host2:9000")));
  }

  @Test(expectedExceptions = RssInvalidStateException.class)
  public void createReplicationGroups_twoReplicas_oneServer() {
    ServerReplicationGroupUtil
        .createReplicationGroups(Arrays.asList(new ServerDetail("server1", "host:9000")), 2);
  }

  @Test
  public void createReplicationGroups_twoReplicas_twoServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"));

    List<ServerReplicationGroup> replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroups(serverDetailList, 2);
    Assert.assertEquals(replicationGroups.size(), 1);
    Assert.assertEquals(replicationGroups.get(0).getServers(), new ArrayList<>(serverDetailList));
  }

  @Test
  public void createReplicationGroups_twoReplicas_fourServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"),
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000"));

    List<ServerReplicationGroup> replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroups(serverDetailList, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    Assert.assertEquals(replicationGroups.get(0).getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000"),
            new ServerDetail("server2", "host2:9000")));
    Assert.assertEquals(replicationGroups.get(1).getServers(),
        Arrays.asList(new ServerDetail("server3", "host3:9000"),
            new ServerDetail("server4", "host4:9000")));
  }

  @Test
  public void createReplicationGroups_twoReplicas_fiveServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"),
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000"),
        new ServerDetail("server5", "host5:9000"));

    List<ServerReplicationGroup> replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroups(serverDetailList, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    Assert.assertEquals(replicationGroups.get(0).getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000"),
            new ServerDetail("server2", "host2:9000")));
    Assert.assertEquals(replicationGroups.get(1).getServers(),
        Arrays.asList(new ServerDetail("server3", "host3:9000"),
            new ServerDetail("server4", "host4:9000")));
  }

  @Test
  public void createReplicationGroups_threeReplicas_fiveServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"),
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000"),
        new ServerDetail("server5", "host5:9000"));

    List<ServerReplicationGroup> replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroups(serverDetailList, 3);
    Assert.assertEquals(replicationGroups.size(), 1);
    Assert.assertEquals(replicationGroups.get(0).getServers(),
        Arrays.asList(
            new ServerDetail("server1", "host:9000"),
            new ServerDetail("server2", "host2:9000"),
            new ServerDetail("server3", "host3:9000")));
  }

  @Test(expectedExceptions = RssInvalidStateException.class)
  public void createReplicationGroupsForPartition_twoServersPerPartition_oneServer() {
    ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        Arrays.asList(new ServerDetail("server1", "host:9000")), 1, 0, 2);
  }

  @Test
  public void createReplicationGroupsForPartition_oneReplica() {
    int partition = 0;
    List<ServerReplicationGroup> replicationGroups = ServerReplicationGroupUtil
        .createReplicationGroupsForPartition(
            Arrays.asList(new ServerDetail("server1", "host:9000")), 1, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    ServerReplicationGroup replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000")));

    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        Arrays.asList(new ServerDetail("server1", "host:9000")), 1, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000")));

    partition = 0;
    replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
            new ServerDetail("server1", "host:9000"),
            new ServerDetail("server2", "host2:9000")),
            1,
            partition,
            1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000")));

    // two servers per partition
    partition = 0;
    replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
            new ServerDetail("server1", "host:9000"),
            new ServerDetail("server2", "host2:9000")),
            1,
            partition,
            2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server2", "host2:9000")));

    partition = 1;
    replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
            new ServerDetail("server1", "host:9000"),
            new ServerDetail("server2", "host2:9000")),
            1,
            partition,
            1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server2", "host2:9000")));

    // two servers per partition
    partition = 1;
    replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
            new ServerDetail("server1", "host:9000"),
            new ServerDetail("server2", "host2:9000")),
            1,
            partition,
            2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server2", "host2:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000")));

    partition = 2;
    replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
            new ServerDetail("server1", "host:9000"),
            new ServerDetail("server2", "host2:9000")),
            1,
            partition,
            1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000")));

    // two servers per partition
    partition = 2;
    replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroupsForPartition(Arrays.asList(
            new ServerDetail("server1", "host:9000"),
            new ServerDetail("server2", "host2:9000")),
            1,
            partition,
            2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server1", "host:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(),
        Arrays.asList(new ServerDetail("server2", "host2:9000")));
  }

  @Test(expectedExceptions = RssInvalidStateException.class)
  public void createReplicationGroup_twoReplicas_oneServer() {
    int partition = 0;
    ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        Arrays.asList(new ServerDetail("server1", "host:9000")), 2, partition, 1);
  }

  @Test(expectedExceptions = RssInvalidStateException.class)
  public void createReplicationGroupsForPartition_twoReplicas_twoServers_twoServersPerPartition() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"));

    int partition = 0;
    List<ServerReplicationGroup> replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroupsForPartition(
            serverDetailList, 2, partition, 2);
  }

  @Test
  public void createReplicationGroupsForPartition_twoReplicas_twoServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"));

    int partition = 0;
    List<ServerReplicationGroup> replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroupsForPartition(
            serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    ServerReplicationGroup replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));

    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));

    partition = 2;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));
  }

  @Test
  public void createReplicationGroupsForPartition_twoReplicas_fiveServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"),
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000"),
        new ServerDetail("server5", "host5:9000"));

    int partition = 0;
    List<ServerReplicationGroup> replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroupsForPartition(
            serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    ServerReplicationGroup replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));

    // two servers per partition
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000")));

    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000")));

    // two servers per partition
    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));

    partition = 2;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));

    // two servers per partition
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000")));

    partition = 3;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000")));

    // two servers per partition
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));

    partition = 4;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));

    // two servers per partition
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 2, partition, 2);
    Assert.assertEquals(replicationGroups.size(), 2);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000")));
    replicationGroup = replicationGroups.get(1);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000")));
  }

  @Test
  public void createReplicationGroupsForPartition_threeReplicas_fiveServers() {
    List<ServerDetail> serverDetailList = Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"),
        new ServerDetail("server3", "host3:9000"),
        new ServerDetail("server4", "host4:9000"),
        new ServerDetail("server5", "host5:9000"));

    int partition = 0;
    List<ServerReplicationGroup> replicationGroups =
        ServerReplicationGroupUtil.createReplicationGroupsForPartition(
            serverDetailList, 3, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    ServerReplicationGroup replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"),
        new ServerDetail("server3", "host3:9000")));

    partition = 1;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 3, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"),
        new ServerDetail("server3", "host3:9000")));

    partition = 2;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 3, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"),
        new ServerDetail("server3", "host3:9000")));

    partition = 3;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 3, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"),
        new ServerDetail("server3", "host3:9000")));

    partition = 4;
    replicationGroups = ServerReplicationGroupUtil.createReplicationGroupsForPartition(
        serverDetailList, 3, partition, 1);
    Assert.assertEquals(replicationGroups.size(), 1);
    replicationGroup = replicationGroups.get(0);
    Assert.assertEquals(replicationGroup.getServers(), Arrays.asList(
        new ServerDetail("server1", "host:9000"),
        new ServerDetail("server2", "host2:9000"),
        new ServerDetail("server3", "host3:9000")));
  }
}
