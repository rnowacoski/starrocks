// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BasePartitionStatisticsScan;
import org.apache.iceberg.PartitionStatistics;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;

/**
 * Tests for partition statistics fallback functionality in IcebergCatalog.
 * These tests verify that when last_updated_at is null from the PARTITIONS table,
 * the system correctly falls back to reading partition statistics files.
 */
public class IcebergPartitionStatsFallbackTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    public static final IcebergCatalogProperties DEFAULT_CATALOG_PROPERTIES;
    public static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();
    public static ConnectContext connectContext;

    static {
        DEFAULT_CONFIG.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        DEFAULT_CONFIG.put(ICEBERG_CATALOG_TYPE, "hive");
        DEFAULT_CATALOG_PROPERTIES = new IcebergCatalogProperties(DEFAULT_CONFIG);
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    /**
     * Test that partitions have valid modified time when snapshot is expired
     * but fallback mechanisms work correctly.
     * This tests the full fallback chain:
     * 1. Try to get last_updated_at from PARTITIONS table
     * 2. If null, try to get from partition statistics files
     * 3. If still null, fallback to current snapshot timestamp
     */
    @Test
    public void testGetPartitionsWithExpiredSnapshotFallbackToSnapshotTime() {
        // Setup: append files, then expire snapshots
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();
        mockedNativeTableB.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit();
        mockedNativeTableB.refresh();

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName) {
                return mockedNativeTableB;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergHiveCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, ImmutableList.of("k2=2", "k2=3"));
        Assertions.assertEquals(2, partitions.size());
        // Verify partition's modified time should not be -1 after fallback
        Assertions.assertTrue(partitions.stream().noneMatch(x -> x.getModifiedTime() == -1),
                "All partitions should have valid modified time after fallback");
    }

    /**
     * Test that when partition statistics scan throws IOException,
     * the system gracefully handles it and falls back to snapshot timestamp.
     */
    @Test
    public void testGetPartitionsWithPartitionStatsScanException() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName) {
                return mockedNativeTableB;
            }
        };

        // Mock BasePartitionStatisticsScan to throw IOException
        new MockUp<BasePartitionStatisticsScan>() {
            @Mock
            public CloseableIterable<PartitionStatistics> scan() throws IOException {
                throw new IOException("Simulated partition statistics scan failure");
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergHiveCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        // Should not throw exception - should gracefully handle and use fallback
        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, ImmutableList.of("k2=2"));
        Assertions.assertEquals(1, partitions.size());
        // Should have valid modified time from snapshot timestamp fallback
        Assertions.assertTrue(partitions.get(0).getModifiedTime() > 0,
                "Partition should have valid modified time from snapshot fallback");
    }

    /**
     * Test that when partition statistics are empty (no stats files),
     * the system falls back to snapshot timestamp correctly.
     */
    @Test
    public void testGetPartitionsWithEmptyPartitionStats() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();
        // Expire snapshots to trigger fallback path
        mockedNativeTableB.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit();
        mockedNativeTableB.refresh();

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName) {
                return mockedNativeTableB;
            }
        };

        // Mock BasePartitionStatisticsScan to return empty results
        new MockUp<BasePartitionStatisticsScan>() {
            @Mock
            public CloseableIterable<PartitionStatistics> scan() {
                return new CloseableIterable<PartitionStatistics>() {
                    @Override
                    public void close() {
                        // no-op
                    }

                    @Override
                    public Iterator<PartitionStatistics> iterator() {
                        return Collections.emptyIterator();
                    }
                };
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergHiveCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, ImmutableList.of("k2=2", "k2=3"));
        Assertions.assertEquals(2, partitions.size());
        // All partitions should have valid modified time from snapshot fallback
        Assertions.assertTrue(partitions.stream().noneMatch(x -> x.getModifiedTime() == -1),
                "All partitions should have valid modified time from snapshot fallback");
    }

    /**
     * Test the normal case where last_updated_at is available directly from PARTITIONS table.
     * In this case, partition statistics should not be consulted.
     */
    @Test
    public void testGetPartitionsWithValidLastUpdatedAt() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName) {
                return mockedNativeTableB;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergHiveCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, ImmutableList.of("k2=2", "k2=3"));
        Assertions.assertEquals(2, partitions.size());
        // All partitions should have valid modified time directly from PARTITIONS table
        Assertions.assertTrue(partitions.stream().noneMatch(x -> x.getModifiedTime() == -1),
                "All partitions should have valid modified time");
        // Modified time should be reasonable (after year 2000)
        long year2000Millis = 946684800000L;
        Assertions.assertTrue(partitions.stream().allMatch(x -> x.getModifiedTime() > year2000Millis),
                "All partition modified times should be after year 2000");
    }

    /**
     * Test unpartitioned table behavior - should still work with fallback mechanism.
     */
    @Test
    public void testGetPartitionsUnpartitionedTableWithFallback() {
        // Use mockedNativeTableG which is unpartitioned (SPEC_B_1 = unpartitioned)
        mockedNativeTableG.newAppend().appendFile(FILE_B_5).commit();
        mockedNativeTableG.refresh();

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName) {
                return mockedNativeTableG;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergHiveCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableG, Maps.newHashMap());

        // For unpartitioned tables, getPartitions should return a single partition with empty name
        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, Lists.newArrayList());
        Assertions.assertEquals(1, partitions.size());
        // Should have valid modified time
        Assertions.assertTrue(partitions.get(0).getModifiedTime() > 0,
                "Unpartitioned table should have valid modified time");
    }
}

