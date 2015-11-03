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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.util.lang.GridFilteredIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.h2.command.dml.Select;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.index.BaseIndex;
import org.h2.index.IndexCondition;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.table.TableView;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.KEY_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2AbstractKeyValueRow.VAL_COL;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2QueryType.PREPARE;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2TableFilterCollocation.PARTITIONED_COLLOCATED;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2TableFilterCollocation.PARTITIONED_FIRST;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2TableFilterCollocation.PARTITIONED_NOT_COLLOCATED;
import static org.apache.ignite.internal.processors.query.h2.opt.GridH2TableFilterCollocation.REPLICATED;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.VIEW_QUERY;

/**
 * Index base.
 */
public abstract class GridH2IndexBase extends BaseIndex {
    /** */
    private static final int MULTIPLIER_COLLOCATED = 1;

    /** */
    private static final int MULTIPLIER_UNICAST = 20;

    /** */
    private static final int MULTIPLIER_BROADCAST = 80;

    /** */
    private static final AtomicLong idxIdGen = new AtomicLong();

    /** */
    protected final long idxId = idxIdGen.incrementAndGet();

    /** */
    private final ThreadLocal<Object> snapshot = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override public final void close(Session session) {
        // No-op. Actual index destruction must happen in method destroy.
    }

    /**
     * Attempts to destroys index and release all the resources.
     * We use this method instead of {@link #close(Session)} because that method
     * is used by H2 internally.
     */
    public abstract void destroy();

    /**
     * If the index supports rebuilding it has to creates its own copy.
     *
     * @return Rebuilt copy.
     * @throws InterruptedException If interrupted.
     */
    public GridH2IndexBase rebuild() throws InterruptedException {
        return this;
    }

    /**
     * Put row if absent.
     *
     * @param row Row.
     * @return Existing row or {@code null}.
     */
    public abstract GridH2Row put(GridH2Row row);

    /**
     * Remove row from index.
     *
     * @param row Row.
     * @return Removed row.
     */
    public abstract GridH2Row remove(SearchRow row);

    /**
     * Takes or sets existing snapshot to be used in current thread.
     *
     * @param s Optional existing snapshot to use.
     * @param qctx Query context.
     * @return Snapshot.
     */
    public final Object takeSnapshot(@Nullable Object s, GridH2QueryContext qctx) {
        assert snapshot.get() == null;

        if (s == null)
            s = doTakeSnapshot();

        if (s != null) {
            if (s instanceof GridReservable && !((GridReservable)s).reserve())
                return null;

            snapshot.set(s);

            if (qctx != null)
                qctx.putSnapshot(idxId, s);
        }

        return s;
    }

    /**
     * @param masks Masks.
     * @param filters All joined table filters.
     * @param filter Current filter.
     * @return Multiplier.
     */
    public int getDistributedMultiplier(int[] masks, TableFilter[] filters, int filter) {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        // We do complex optimizations with respect to distributed joins only on prepare stage
        // because on run stage reordering of joined tables by Optimizer is explicitly disabled
        // and thus multiplier will be always the same, so it will not affect choice of index.
        if (qctx == null || qctx.type() != PREPARE || !qctx.distributedJoins())
            return MULTIPLIER_COLLOCATED;

        Map<TableFilter,GridH2TableFilterCollocation> states = qctx.tableFilterStateCache();

        assert states != null;

        // Need to do this clean up because subquery states can be outdated here.
        clearPreviousSubQueryStates(filters, filter, states);

        final TableFilter f = filters[filter];

        // Only partitioned tables will do distributed joins.
        if (!getTable().isPartitioned()) {
            states.put(f, REPLICATED);

            return MULTIPLIER_COLLOCATED;
        }

        // If we are the first partitioned table in a join, then we are "base" for all the rest partitioned tables
        // which will need to get remote result (if there is no affinity condition). Since this query is broadcasted
        // to all the affinity nodes the "base" does not need to get remote results.
        if (!findPartitionedTableBefore(filters, filter, states)) {
            states.put(f, PARTITIONED_FIRST);

            return MULTIPLIER_COLLOCATED;
        }

        int affColId = affinityColumn();

        // If we don't have affinity equality conditions then most probably we will have to broadcast.
        if (!hasEqualityCondition(masks, affColId)) {
            states.put(f, PARTITIONED_NOT_COLLOCATED);

            return MULTIPLIER_BROADCAST;
        }

        // If we have an affinity condition then we have to check if the whole join chain is collocated so far.
        ArrayList<IndexCondition> idxConditions = f.getIndexConditions();

        for (int i = 0; i < idxConditions.size(); i++) {
            IndexCondition c = idxConditions.get(i);

            if (c.getCompareType() == IndexCondition.EQUALITY &&
                c.getColumn().getColumnId() == affColId && c.isEvaluatable()) {
                Expression exp = c.getExpression();

                if (exp instanceof ExpressionColumn) {
                    ExpressionColumn expCol = (ExpressionColumn)exp;

                    // This is one of our previous joins.
                    TableFilter join = expCol.getTableFilter();

                    GridH2TableFilterCollocation state = states.get(join);

                    assert state != null : "all the previous states must be calculated already";

                    if (state.isPartitioned() && state.isCollocated()) {
                        GridH2Table joinTbl = (GridH2Table)expCol.getColumn().getTable();

                        if (joinTbl.getAffinityKeyColumn().column.getColumnId() == expCol.getColumn().getColumnId()) {
                            states.put(f, PARTITIONED_COLLOCATED);

                            return MULTIPLIER_COLLOCATED;
                        }
                    }
                }
            }
        }

        states.put(f, PARTITIONED_NOT_COLLOCATED);

        return MULTIPLIER_UNICAST;
    }

    /**
     * @param filters All joined table filters.
     * @param filter Current filter.
     * @param states States map.
     */
    private static void clearPreviousSubQueryStates(TableFilter[] filters, int filter,
        Map<TableFilter,GridH2TableFilterCollocation> states) {
        // We have to go back and clean up state for all the previous subqueries.
        for (int i = filter - 1; i >= 0; i--) {
            TableFilter f0 = filters[i];

            if (f0.getTable() instanceof TableView)
                states.put(f0, null);
            else
                break;
        }
    }

    /**
     * @param filters All joined table filters.
     * @param filter Current filter.
     * @param states States map.
     * @return {@code true} If there are partitioned table before.
     */
    private static boolean findPartitionedTableBefore(TableFilter[] filters, int filter,
        Map<TableFilter,GridH2TableFilterCollocation> states) {
        // Go backwards to calculate dropped subquery states.
        for (int i = filter - 1; i >= 0; i--) {
            TableFilter prevFilter = filters[i];

            GridH2TableFilterCollocation state = states.get(prevFilter);

            if (state == null) // This can happen if previous filter is a subquery or function.
                state = getStateForNonTable(prevFilter, states);

            if (state.isPartitioned())
                return true;
        }

        return false;
    }

    /**
     * @param filter Filter.
     * @param states States map.
     * @return Filter collocation.
     */
    private static GridH2TableFilterCollocation getStateForNonTable(TableFilter filter,
        Map<TableFilter,GridH2TableFilterCollocation> states) {
        Table tbl = filter.getTable();

        assert !(tbl instanceof GridH2Table);

        GridH2TableFilterCollocation res = null;

        if (tbl instanceof TableView) {
            Select select = (Select)VIEW_QUERY.get((TableView)tbl);

            ArrayList<TableFilter> filters = select.getTopFilters();

            int partitioned = -1;

            for (int i = 0; i < filters.size(); i++) {
                TableFilter f = filters.get(i);

                GridH2TableFilterCollocation state = states.get(f);

                if (state == null)
                    state = getStateForNonTable(f, states);

                if (!state.isCollocated()) {
                    res = PARTITIONED_NOT_COLLOCATED;

                    break;
                }

                if (state.isPartitioned())
                    partitioned = i;
            }

            if (res == null) {
                if (partitioned == -1)
                    return REPLICATED;

                res = partitioned == 0 ? PARTITIONED_FIRST : PARTITIONED_COLLOCATED;
            }
        }
        else {
            // It is a some kind of function or system table.
            res = REPLICATED;
        }

        assert res != null;

        states.put(filter, res);

        return res;
    }

    /**
     * @return Affinity column.
     */
    protected int affinityColumn() {
        return getTable().getAffinityKeyColumn().column.getColumnId();
    }

    /**
     * @param masks Masks.
     * @param colId Column ID.
     * @return {@code true} If set of index conditions contains equality condition for the given column.
     */
    private static boolean hasEqualityCondition(int[] masks, int colId) {
        return masks != null && (masks[colId] & IndexCondition.EQUALITY) == IndexCondition.EQUALITY;
    }

    /** {@inheritDoc} */
    @Override public GridH2Table getTable() {
        return (GridH2Table)super.getTable();
    }

    /**
     * Takes and returns actual snapshot or {@code null} if snapshots are not supported.
     *
     * @return Snapshot or {@code null}.
     */
    @Nullable protected abstract Object doTakeSnapshot();

    /**
     * @return Thread local snapshot.
     */
    @SuppressWarnings("unchecked")
    protected <T> T threadLocalSnapshot() {
        return (T)snapshot.get();
    }

    /**
     * Releases snapshot for current thread.
     */
    public void releaseSnapshot() {
        Object s = snapshot.get();

        assert s != null;

        snapshot.remove();

        if (s instanceof GridReservable)
            ((GridReservable)s).release();

        if (s instanceof AutoCloseable)
            U.closeQuiet((AutoCloseable)s);
    }

    /**
     * Filters rows from expired ones and using predicate.
     *
     * @param iter Iterator over rows.
     * @return Filtered iterator.
     */
    protected Iterator<GridH2Row> filter(Iterator<GridH2Row> iter) {
        IgniteBiPredicate<Object, Object> p = null;

        IndexingQueryFilter f = filter();

        if (f != null) {
            String spaceName = getTable().spaceName();

            p = f.forSpace(spaceName);
        }

        return new FilteringIterator(iter, U.currentTimeMillis(), p);
    }

    /**
     * @return Filter for currently running query or {@code null} if none.
     */
    protected IndexingQueryFilter filter() {
        GridH2QueryContext qctx = GridH2QueryContext.get();

        return qctx == null ? null : qctx.filter();
    }

    /** {@inheritDoc} */
    @Override public long getDiskSpaceUsed() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void checkRename() {
        throw DbException.getUnsupportedException("rename");
    }

    /** {@inheritDoc} */
    @Override public void add(Session ses, Row row) {
        throw DbException.getUnsupportedException("add");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses, Row row) {
        throw DbException.getUnsupportedException("remove row");
    }

    /** {@inheritDoc} */
    @Override public void remove(Session ses) {
        throw DbException.getUnsupportedException("remove index");
    }

    /** {@inheritDoc} */
    @Override public void truncate(Session ses) {
        throw DbException.getUnsupportedException("truncate");
    }

    /** {@inheritDoc} */
    @Override public boolean needRebuild() {
        return false;
    }

    /**
     * Iterator which filters by expiration time and predicate.
     */
    protected static class FilteringIterator extends GridFilteredIterator<GridH2Row> {
        /** */
        private final IgniteBiPredicate<Object, Object> fltr;

        /** */
        private final long time;

        /**
         * @param iter Iterator.
         * @param time Time for expired rows filtering.
         */
        protected FilteringIterator(Iterator<GridH2Row> iter, long time,
            IgniteBiPredicate<Object, Object> fltr) {
            super(iter);

            this.time = time;
            this.fltr = fltr;
        }

        /**
         * @param row Row.
         * @return If this row was accepted.
         */
        @SuppressWarnings("unchecked")
        @Override protected boolean accept(GridH2Row row) {
            if (row instanceof GridH2AbstractKeyValueRow) {
                if (((GridH2AbstractKeyValueRow) row).expirationTime() <= time)
                    return false;
            }

            if (fltr == null)
                return true;

            Object key = row.getValue(KEY_COL).getObject();
            Object val = row.getValue(VAL_COL).getObject();

            assert key != null;
            assert val != null;

            return fltr.apply(key, val);
        }
    }
}