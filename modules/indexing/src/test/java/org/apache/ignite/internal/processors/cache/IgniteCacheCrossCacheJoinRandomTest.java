/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.h2.sql.AbstractH2CompareQueryTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheCrossCacheJoinRandomTest extends AbstractH2CompareQueryTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private static final int OBJECTS = 200;

    /** */
    private static final int CACHES = 5;

    /** */
    private Random rnd;

    /** {@inheritDoc} */
    @Override protected void setIndexedTypes(CacheConfiguration<?, ?> cc, CacheMode mode) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void initCacheAndDbData() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void checkAllDataEquals() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = ((TcpDiscoverySpi)cfg.getDiscoverySpi());

        spi.setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
    }
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        long seed = System.currentTimeMillis();

        rnd = new Random(seed);

        log.info("Random seed: " + seed);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        client = true;

        startGrid(SRVS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected Statement initializeH2Schema() throws SQLException {
        Statement st = super.initializeH2Schema();

        for (int i = 0; i < CACHES; i++) {
            st.execute("CREATE SCHEMA \"cache" + i + "\"");

            st.execute("create table \"cache" + i + "\".TESTOBJECT" +
                "  (_key int not null," +
                "  _val other not null," +
                "  parentId int)");
        }

        return st;
    }

    /**
     * @param name Cache name.
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration configuration(String name, CacheMode cacheMode, int backups) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setCacheMode(cacheMode);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        QueryEntity entity = new QueryEntity();
        entity.setKeyType(Integer.class.getName());
        entity.setValueType(TestObject.class.getName());
        entity.addQueryField("parentId", Integer.class.getName(), null);
        entity.setIndexes(F.asList(new QueryIndex("parentId")));

        ccfg.setQueryEntities(F.asList(entity));

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoin() throws Exception {
        List<Map<Integer, Integer>> cachesData = new ArrayList<>(CACHES);

        for (int i = 0; i < CACHES; i++) {
            Map<Integer, Integer> data = createData(OBJECTS / 2);

            insertH2(data, i);

            cachesData.add(data);
        }

        List<T2<CacheMode, Integer>> allModes = F.asList(
            new T2<>(REPLICATED, 0),
            new T2<>(PARTITIONED, 1),
            new T2<>(PARTITIONED, 2),
            new T2<>(PARTITIONED, 3));

        checkJoin(cachesData, allModes, new Stack<T2<CacheMode, Integer>>(), CACHES);
    }

    /**
     * @param cachesData Caches data.
     * @param allModes Modes to test.
     * @param modes Select modes.
     * @param caches Caches number.
     * @throws Exception If failed.
     */
    private void checkJoin(List<Map<Integer, Integer>> cachesData,
        List<T2<CacheMode, Integer>> allModes,
        Stack<T2<CacheMode, Integer>> modes,
        int caches) throws Exception {
        if (modes.size() == caches) {
            List<CacheConfiguration> ccfgs = new ArrayList<>();

            for (int i = 0; i < modes.size(); i++) {
                T2<CacheMode, Integer> mode = modes.get(i);

                CacheConfiguration ccfg = configuration("cache" + i, mode.get1(), mode.get2());

                ccfgs.add(ccfg);
            }

            log.info("Check configurations: " + modes);

            checkJoin(ccfgs, cachesData);
        }
        else {
            for (T2<CacheMode, Integer> mode : allModes) {
                modes.push(mode);

                checkJoin(cachesData, allModes, modes, caches);

                modes.pop();
            }
        }
    }

    /**
     * @param ccfgs Configurations.
     * @param cachesData Caches data.
     * @throws Exception If failed.
     */
    private void checkJoin(List<CacheConfiguration> ccfgs, List<Map<Integer, Integer>> cachesData) throws Exception {
        Ignite client = ignite(SRVS);

        try {
            IgniteCache cache = null;

            for (int i = 0; i < CACHES; i++) {
                CacheConfiguration ccfg = ccfgs.get(i);

                IgniteCache cache0 = client.createCache(ccfg);

                if (cache == null && ccfg.getCacheMode() == PARTITIONED)
                    cache = cache0;

                insertCache(cachesData.get(i), cache0);
            }

            boolean distributedJoin = true;

            if (cache == null) {
                cache = client.cache(ccfgs.get(0).getName());

                distributedJoin = false;
            }

            Object[] args = {};

            compareQueryRes0(cache, createQuery(CACHES, null), distributedJoin, true, args, Ordering.RANDOM);

            Map<Integer, Integer> data = cachesData.get(CACHES - 1);

            for (Integer objId : data.keySet())
                compareQueryRes0(cache, createQuery(CACHES, objId), distributedJoin, true, args, Ordering.RANDOM);
        }
        finally {
            for (CacheConfiguration ccfg : ccfgs)
                client.destroyCache(ccfg.getName());
        }
    }

    /**
     * @param caches Number of caches to join.
     * @param objId Object ID.
     * @return SQL.
     */
    private String createQuery(int caches, @Nullable Integer objId) {
        StringBuilder qry = new StringBuilder("select ");

        for (int i = 0; i < caches; i++) {
            if (i != 0)
                qry.append(", ");

            qry.append("o" + i + "._key");
        }

        qry.append(" from \"cache0\".TestObject o0 ");

        for (int i = 1; i < caches; i++) {
            String cacheName = "cache" + i;

            String cur = "o" + i;
            String prev = "o" + (i - 1);


            qry.append("join \"" + cacheName + "\".TestObject " + cur);

            if (i == caches - 1 && objId != null)
                qry.append(" on (" + prev + ".parentId=" + cur + "._key and " + prev + "._key=" + objId + ") ");
            else
                qry.append(" on (" + prev + ".parentId=" + cur + "._key) ");
        }

        return qry.toString();
    }

    /**
     * @param data Data.
     * @param cache Cache.
     */
    private void insertCache(Map<Integer, Integer> data, IgniteCache<Object, Object> cache) {
        for (Map.Entry<Integer, Integer> e : data.entrySet())
            cache.put(e.getKey(), new TestObject(e.getValue()));
    }

    /**
     * @param data Data.
     * @param cache Cache index.
     * @throws Exception If failed.
     */
    private void insertH2(Map<Integer, Integer> data, int cache) throws Exception {
        for (Map.Entry<Integer, Integer> e : data.entrySet()) {
            try(PreparedStatement st = conn.prepareStatement("insert into \"cache" + cache + "\".TESTOBJECT " +
                "(_key, _val, parentId) values(?, ?, ?)")) {
                st.setObject(1, e.getKey());
                st.setObject(2, new TestObject(e.getValue()));
                st.setObject(3, e.getValue());

                st.executeUpdate();
            }
        }
    }

    /**
     * @param cnt Objects count.
     * @return Generated data.
     */
    private Map<Integer, Integer> createData(int cnt) {
        assert cnt <= OBJECTS : cnt;

        Map<Integer, Integer> res = new HashMap<>();

        while (res.size() < cnt)
            res.put(rnd.nextInt(OBJECTS), rnd.nextInt(OBJECTS));

        return res;
    }

    /**
     *
     */
    static class TestObject implements Serializable {
        /** */
        int parentId;

        /**
         * @param parentId Parent object ID.
         */
        public TestObject(int parentId) {
            this.parentId = parentId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestObject.class, this);
        }
    }
}
