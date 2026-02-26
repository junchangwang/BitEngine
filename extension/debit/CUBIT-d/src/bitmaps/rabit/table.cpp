#include <iostream>
#include <tuple>
#include <atomic>
#include "fastbit/bitvector.h"
#include "utils/util.h"
#include "bitmaps/rabit/segBtv.h"
#include "bitmaps/rabit/table.h"

using namespace std;
using namespace rabit;

Rabit::Rabit(Table_config *config) : BaseTable(config), 
            g_timestamp(TIMESTAMP_INIT_VAL), g_number_of_rows(config->n_rows)
{
    autoCommit = config->autoCommit;
    merge_threshold = config->n_merge_threshold;
    db_control = config->db_control;

    if (config->encoding == EE || config->encoding == GE) {
        num_btvs = (config->file_format == "bm" ? config->g_cardinality : (config->g_cardinality + config->zip_num - 1) / config->zip_num);
        min_value_EE = config->min_value_EE;
        max_value_EE = config->g_cardinality - 1;
    }
        
    else if (config->encoding == RE) 
        num_btvs = config->g_cardinality - 1;
    else 
        assert(0);

    if (config->encoding == GE) {
        num_btvs_GE = (config->g_cardinality + config->GE_group_len - 1) / config->GE_group_len;
    } else {
        num_btvs_GE = 0;
    }

    TransDesc *trans_dummy = new TransDesc();

    trans_dummy->l_start_ts = 0UL;
    if (!db_control) {
        // Stand-alone RABIT
        uint128_t data_t = __atomic_load_n((uint128_t *)&g_timestamp, MM_RELAXED);
        trans_dummy->l_commit_ts = (uint64_t)data_t;
        trans_dummy->l_number_of_rows = (uint64_t)(data_t>>64);
    } else {
        // Serve as a library in DBMS systems like DBx1000
        uint64_t data_t = __atomic_load_n(&db_timestamp, MM_RELAXED);
        trans_dummy->l_commit_ts = data_t;
        trans_dummy->l_number_of_rows = db_number_of_rows;
    } 

    assert(trans_dummy->l_commit_ts == TIMESTAMP_INIT_VAL);
    assert(trans_dummy->l_number_of_rows == config->n_rows);

    Btvs = new Bitvector*[config->g_cardinality];
    Btvs_GE = new Bitvector*[num_btvs_GE];

    total_rows = config->n_rows + RABIT_PAD_BITS;

    if(config->encoding == EE) {
        int n_threads = (config->nThreads_for_getval > num_btvs) ? 
                            num_btvs : config->nThreads_for_getval;
        int n_btv_per_thread = num_btvs / n_threads;
        assert(n_btv_per_thread >= 1);

        int reminder = num_btvs % n_threads;
        vector<int> begin (n_threads + 1, 0);
        for(int i = 1; i <= reminder; i++)
            begin[i] = begin[i - 1] + n_btv_per_thread + 1;
        for(int i = reminder + 1; i <= n_threads; i++)
            begin[i] = begin[i - 1] + n_btv_per_thread;

        thread* threads = new thread[n_threads];
        if (config->file_format == "bm") {
            for (int i = 0; i < n_threads; i++) {
                threads[i] = thread(&Rabit::_load_btv, this, begin[i], 
                                    begin[i + 1], trans_dummy, config);
            }
        }
        else {
            for (int i = 0; i < n_threads; i++) {
                threads[i] = thread(&Rabit::_load_zipbtv, this, begin[i], 
                                    begin[i + 1], trans_dummy, config);
            }
        }
        

        for (int t = 0; t < n_threads; t++) {
            threads[t].join();
        }

        delete[] threads;
    }

    //Load GE bitvectors
    if(config->encoding == GE) {
        int n_threads = (config->nThreads_for_getval > num_btvs_GE) ?
                        num_btvs_GE : config->nThreads_for_getval;
        int n_btv_per_thread = num_btvs_GE / n_threads;
        int reminder = num_btvs_GE % n_threads;
        assert(n_btv_per_thread >= 1);

        vector<int>begin (n_threads + 1, 0);
        for(int i = 1; i <= reminder; i++)
            begin[i] = begin[i - 1] + n_btv_per_thread + 1;
        for(int i = reminder + 1; i <= n_threads; i++)
            begin[i] = begin[i - 1] + n_btv_per_thread;

        thread* threads = new thread[n_threads];
        for (int i = 0; i < n_threads; i++) {
            threads[i] = thread(&Rabit::_load_btv_GE, this, begin[i], begin[i + 1], trans_dummy, config);
        }

        for (int t = 0; t < n_threads; t++) {
            threads[t].join();
        }
        delete[] threads;
    }

    n_trans_pool = config->n_workers*(config->n_udis+config->n_queries)*2*10;
    trans_pool = new TransDesc[n_trans_pool] {};
    assert(trans_pool);
    __atomic_store_n(&cnt_trans_used, 0, MM_RELAXED);

    if (config->segmented_btv) {
        uint64_t btv_size_t = 0;
        if (config->encoding == EE) {
            for (int i = 0; i < num_btvs; i++) {
                for (const auto &[id_t, seg_t] : Btvs[i]->seg_btv->seg_table) {
                    btv_size_t += seg_t->btv->getSerialSize();
                }
            }
        }
        if (config->encoding == GE) {
            for (int i = 0; i < num_btvs_GE; i++) {
                for (const auto &[id_t, seg_t] : Btvs_GE[i]->seg_btv->seg_table) {
                    btv_size_t += seg_t->btv->getSerialSize();
                }
            }
        }

        cout << "Bitmap size (MB): " << btv_size_t/1000000 << endl;
    }

    trans_queue = new queue_t{};
    if (config->encoding == EE) {
        __atomic_store_n(&trans_queue->head, Btvs[0]->log_shortcut, MM_RELAXED);
        __atomic_store_n(&trans_queue->tail, Btvs[0]->log_shortcut, MM_RELAXED);
    }
    else if (config->encoding == GE) {
        __atomic_store_n(&trans_queue->head, Btvs_GE[0]->log_shortcut, MM_RELAXED);
        __atomic_store_n(&trans_queue->tail, Btvs_GE[0]->log_shortcut, MM_RELAXED);
    }
    merge_cursor_ = trans_queue->head;

    cout << "=== Size of TransDesc pool: " << n_trans_pool 
        << "  autoCommit: " << autoCommit
        << "  merge_threshold: " << merge_threshold 
        << "  db_control: " << db_control
        << "  Segmented: " << config->segmented_btv
        << "  Segment Size: " << config->rows_per_seg
        << "  Enable Parallel CNT: " << config->enable_parallel_cnt
        << "  Para CNT threads: " << config->nThreads_for_getval
        << " ==="<< endl;
}

std::vector<std::pair<uint32_t, uint32_t>> read_ofz(const char* fn) {
    std::vector<std::pair<uint32_t, uint32_t>> result;
    FILE* ofz = fopen(fn, "rb");
    if (!ofz) return result;

    uint32_t offset = 0;
    uint32_t len = 0;
    while (fread(&len, sizeof(uint32_t), 1, ofz) == 1) {
        result.emplace_back(offset, len);
        offset += len;
    }
    fclose(ofz);
    return result;
}

void Rabit::_load_btv(int begin, int end, TransDesc* trans_dummy, Table_config *config) 
{
    for (int i = begin; i < end; i++) {
        Btvs[i] = new Bitvector{};
        __atomic_store_n(&Btvs[i]->log_shortcut, trans_dummy, MM_RELEASE);
        ibis::bitvector *btv_t = new ibis::bitvector();
        assert(btv_t);
        if (config->INDEX_PATH != "")
            btv_t->read(getBtvName(i).c_str());
        btv_t->adjustSize(0, total_rows);
        if (config->enable_fence_pointer) {
            btv_t->index.clear();
            btv_t->buildIndex();
        }

        Btvs[i]->btv = btv_t;
        if(config->segmented_btv) {
            Btvs[i]->seg_btv = new SegBtv(config, btv_t);
            if (config->enable_fence_pointer) {
                Btvs[i]->seg_btv->buildAllIndex();
        }
        }
        
        Btvs[i]->next = NULL;

        // for rabit without db, we cen delete unsegmented bitvectors
        if(!db_control && config->segmented_btv) {
            delete btv_t;
        }
    }
}

void Rabit::_load_zipbtv(int begin, int end, TransDesc* trans_dummy, Table_config* config) 
{   
    int cardinality = config->g_cardinality;
    int i = begin * config->zip_num;
    for (int j = begin; j < end; j++) 
    {   
        std::stringstream temp_offset;
        temp_offset << config->INDEX_PATH << "/" << j << ".ofz";
        auto places = read_ofz(temp_offset.str().c_str());

        int zip_num = (cardinality - i >= config->zip_num ? config->zip_num : (cardinality - i));

        for(int k = 0; k < zip_num; k++, i++) {
            Btvs[i] = new Bitvector{};
            __atomic_store_n(&Btvs[i]->log_shortcut, trans_dummy, MM_RELEASE);
            ibis::bitvector *btv_t = new ibis::bitvector();
            assert(btv_t);

            btv_t->read_bmz(getBtvName(j).c_str(), places[k]);

            btv_t->adjustSize(0, total_rows);
            if (config->enable_fence_pointer) {
                btv_t->index.clear();
                btv_t->buildIndex();
            }

            Btvs[i]->btv = btv_t;
            if(config->segmented_btv) {
                Btvs[i]->seg_btv = new SegBtv(config, btv_t);
                if (config->enable_fence_pointer) {
                    Btvs[i]->seg_btv->buildAllIndex();
                }
            }

            Btvs[i]->next = NULL;

            // for rabit without db, we cen delete unsegmented bitvectors
            if(!db_control && config->segmented_btv) {
                delete btv_t;
            }
        }        
    }
}

void Rabit::_load_btv_GE(int begin, int end, TransDesc* trans_dummy, Table_config *config) 
{
    for (int i = begin; i < end; i++) {
        Btvs_GE[i] = new Bitvector{};
        __atomic_store_n(&Btvs_GE[i]->log_shortcut, trans_dummy, MM_RELEASE);
        ibis::bitvector *btv_t = new ibis::bitvector();
        assert(btv_t);
        if (config->GROUP_PATH != "")
            btv_t->read(getGroupName(i).c_str());
        else {
            printf("GE path is null\n");
            //exit(-1);
            return ;
        }
        btv_t->adjustSize(0, total_rows);
        if (config->enable_fence_pointer) {
            btv_t->index.clear();
            btv_t->buildIndex();
        }

        Btvs_GE[i]->btv = btv_t;
        if(config->segmented_btv) {
            Btvs_GE[i]->seg_btv = new SegBtv(config, btv_t);
            if (config->enable_fence_pointer) {
                Btvs_GE[i]->seg_btv->buildAllIndex();
            }
        }
        

        Btvs_GE[i]->next = NULL;

        // for rabit without db, we cen delete unsegmented bitvectors
        if(!db_control && config->segmented_btv) {
            delete btv_t;
        }


    }
}

/*************************************
 *       Transaction Semantics       *
 ************************************/

TransDesc * Rabit::trans_begin(int tid, uint64_t db_timestamp_t)
{
    RABIT_ThreadInfo *th = &g_ths_info[tid];

    if (READ_ONCE(th->active_trans))
        return (TransDesc *)th->active_trans;

    TransDesc *trans = allocate_trans();
    trans->l_end_trans = __atomic_load_n(&trans_queue->tail, MM_ACQUIRE);
    trans->l_commit_ts = INV_TIMESTAMP;

    if (!db_control) {
        uint128_t data_t = __atomic_load_n((uint128_t *)&g_timestamp, MM_RELAXED);
        trans->l_start_ts = (uint64_t)data_t;
        trans->l_number_of_rows = (uint64_t)(data_t>>64);

        // We need to retrieve a snapshot of the <trans_queue->tail, g_timestamp> pair.
        // However, new trans may have been inserted into TRX_LOG between accessing trans_queue->tail and g_timestamp.
        // We can use the following logic to detect and prevent this, 
        // because RABIT guarantees to update tail and then timestamp, in order.
        while (trans->l_start_ts > READ_ONCE(trans->l_end_trans->l_commit_ts)) {
            assert(READ_ONCE(trans->l_end_trans->next));
            trans->l_end_trans = __atomic_load_n(&trans->l_end_trans->next, MM_ACQUIRE);
        }
    }
    else {
        if(db_timestamp_t != UINT64_MAX) {
            trans->l_start_ts = db_timestamp_t;
        }
        else {
            trans->l_start_ts = __atomic_load_n(&db_timestamp, MM_RELAXED); 
        }
        trans->l_number_of_rows = db_number_of_rows;
    }

    if (trans->l_start_ts < READ_ONCE(trans->l_end_trans->l_commit_ts)) {
        // New trans have completed
        trans->l_start_ts = READ_ONCE(trans->l_end_trans->l_commit_ts);
    }

    trans->l_inc_rows = 0;
    trans->next = NULL;
    __atomic_store_n(&th->active_trans, trans, MM_RELEASE);

    return (TransDesc *)th->active_trans;
}

int Rabit::trans_commit(int tid, uint64_t db_timestamp_t, uint64_t db_row_nums) 
{
    RABIT_ThreadInfo *th = &g_ths_info[tid];
    TransDesc *trans = (TransDesc *)__atomic_load_n(&th->active_trans, MM_ACQUIRE);

    if (!trans) {
#if defined(VERIFY_RESULTS)
        cout << "ERROR: committing an empty trans." << endl;
#endif
        return -EPERM;
    }

    // No updates in this transaction
    if (trans->rubs.empty()) {
        delete_trans(tid, trans);
        __atomic_store_n(&th->active_trans, NULL, MM_RELAXED);
        return -ENOENT;
    }

    {   lock_guard<shared_mutex> guard(g_trans_lk);

        // Check after locking

        struct TransDesc *tail_t = __atomic_load_n(&trans_queue->tail, MM_ACQUIRE);
        if (check_conflicts(trans, tail_t) != 0) {
            delete_trans(tid, trans);
            __atomic_store_n(&th->active_trans, NULL, MM_RELAXED);
            return -EPERM;
        }

        // Passed check
        if (!db_control) {
            // Stand-alone RABIT
            uint128_t data_t = __atomic_load_n((uint128_t *)&g_timestamp, MM_CST);
            trans->l_commit_ts = (uint64_t)data_t + 1;
            trans->l_number_of_rows = (uint64_t)(data_t>>64);
        } else {
            // Serve as a library in DBMS systems like DBx1000
            if(db_timestamp_t != UINT64_MAX) {
                assert(db_row_nums != UINT64_MAX);
                trans->l_commit_ts = db_timestamp_t;
                trans->l_number_of_rows = db_row_nums;
            }
            else {
                uint64_t data_t = __atomic_load_n(&db_timestamp, MM_CST);
                trans->l_commit_ts = data_t + 1;
                trans->l_number_of_rows = db_number_of_rows;
            }
        } 

        // Fill missing row_id's
        if (trans->l_inc_rows) {
            int n_inc_rows = 0;
            map<uint64_t, RUB> row_id_v{};

            // The follwing code snippet can be included in a single for-loop body in the future.
            for (const auto & [row_id_t, rub_t] : trans->rubs) {
                if (rub_t.type == TYPE_INSERT) {
                    row_id_v[row_id_t] = rub_t;
                }
            }

            for (const auto & [row_id_t, rub_t] : row_id_v)
                trans->rubs.erase(row_id_t);

            for (const auto & [row_id_t, rub_t] : row_id_v) {
                auto actual_row_id = trans->l_number_of_rows + n_inc_rows;
                trans->rubs[actual_row_id] = RUB{actual_row_id, rub_t.type, rub_t.pos};
                n_inc_rows ++;
            }
            if(db_timestamp_t == UINT64_MAX) {
                assert(n_inc_rows == trans->l_inc_rows);
            }
        }

        // Link the new trans to trans_queue->tail
        // It is safe to update trans_queue->tail because we are holding the lock.
        __atomic_store_n(&trans_queue->tail->next, trans, MM_CST);
        __atomic_store_n(&trans_queue->tail, trans, MM_CST);

        // Update g_number_of_rows
        __atomic_add_fetch(&g_number_of_rows, trans->l_inc_rows, __ATOMIC_SEQ_CST);

        // Update local timestamp and g_timestamp
        __atomic_add_fetch(&g_timestamp, 1, __ATOMIC_SEQ_CST);

        // NOTE: When RABIT is controlled by a DBMS (i.e., db_control == true),
        //       it's the DBMS system's responsibility to increment db_timestamp.

        __atomic_store_n(&th->active_trans, NULL, MM_RELAXED);
    }

    return 0;
}

/*************************************
 *         Buffer Management         *
 ************************************/

TransDesc * Rabit::allocate_trans() 
{
    uint64_t pos = __atomic_fetch_add(&cnt_trans_used, 1, MM_CST);
    assert(pos < n_trans_pool);

    trans_pool[pos].pos = pos;

    return &trans_pool[pos];
}

int Rabit::delete_trans(int tid, TransDesc *trans) 
{
    // FIXME: No recycle yet
    //
    return 0;
}

/* Get RUBs in between (tsp_begin, tsp_end] */
TransDesc * Rabit::get_rubs_on_btv(uint64_t tsp_begin, uint64_t tsp_end, 
                        TransDesc *trans, uint32_t val, map<uint64_t, RUB> &rubs)
{
    assert(tsp_begin <= tsp_end);
    assert(trans);

    // Check trans in between (tsp_begin, tsp_end].
    TransDesc *trans_prev = trans;
    trans = READ_ONCE(trans->next);
    while (__atomic_load_n(&trans, MM_ACQUIRE) && (READ_ONCE(trans->l_commit_ts) <= tsp_end)) {
        if (READ_ONCE(trans->l_commit_ts) > tsp_begin) 
        {   // The normal case
            for (const auto & [row_id_t, rub_t] : trans->rubs)
            {
                if (rub_t.pos.count(val)) {
                    // perv trans just change once
                    if(rubs.count(row_id_t) && rubs[row_id_t].pos.count(val))
                        rubs.erase(row_id_t);
                    else
                    {
                        rubs[row_id_t] = RUB{row_id_t, rub_t.type, Btv_set{val}};
                    }
                }
            }
        } else if (READ_ONCE(trans->l_commit_ts) == tsp_begin) {
            ; // Skip the first trans
        } else {
            if(db_control)
                assert(trans->l_commit_ts == trans->l_start_ts);
            else assert(false);
        }

        trans_prev = trans;
        trans = READ_ONCE(trans->next);
    }

    return trans ? trans : trans_prev;
}

/* Get the last RUB in between (tsp_begin, tsp_end] of the specified row. */
TransDesc * Rabit::get_rub_on_row(uint64_t tsp_begin, uint64_t tsp_end, 
                        TransDesc *trans, uint64_t row_id, RUB &rub, uint64_t &rub_tsp)
{
    assert(tsp_begin <= tsp_end);
    assert(trans);

    // Check trans in between (tsp_begin, tsp_end].
    TransDesc *trans_prev = trans;
    trans = READ_ONCE(trans->next);
    while (__atomic_load_n(&trans, MM_ACQUIRE) && (READ_ONCE(trans->l_commit_ts) <= tsp_end)) {
        if (READ_ONCE(trans->l_commit_ts) > tsp_begin) 
        {
            for (const auto & [row_id_t, rub_t] : trans->rubs) 
            {
                if (row_id == row_id_t)
                {
                    for(const auto position : rub_t.pos)
                    {
                        if(rub.pos.count(position))
                            rub.pos.erase(position);
                        else
                        {
                            rub.row_id = row_id_t;
                            rub.type = rub_t.type;
                            rub.pos.insert(position);
                        }
                    }
                }
            }
        } else if (trans->l_commit_ts == tsp_begin) {
            ; // Skip the first trans
        } else {
            assert(false);
        }

        trans_prev = trans;
        trans = READ_ONCE(trans->next);
    }

    return trans ? trans : trans_prev;
}

// Check if trans conflicts with others in between (trans->l_end_trans, tail].
// Return 0 if no conflict; Otherwise -1.
// Side effect: Assign tail to trans->l_end_trans if there is no conflict,
//          such that the next invocation of this function on the same trans can be accelerated.
//          No concurrency issue because this function is invoked sequentially.
int Rabit::check_conflicts(TransDesc *trans, TransDesc *tail)
{
    TransDesc *end_trans_t = __atomic_load_n(&trans->l_end_trans, MM_ACQUIRE);
    TransDesc *end_trans_t_2 = __atomic_load_n(&end_trans_t->next, MM_ACQUIRE);

    while (end_trans_t_2 && READ_ONCE(end_trans_t_2->l_commit_ts) <= READ_ONCE(tail->l_commit_ts)) 
    {
        for (const auto & [row_t, rub_t] : trans->rubs) {
            // No need to check the rubs of insert operations since they will be updated 
            // to the correct row ids when submitting.
            if (rub_t.type != TYPE_INSERT) {
                if (end_trans_t_2->rubs.count(row_t)) {
                    #if defined(VERIFY_RESULTS)
                    cout << "NOTE: In check_conflict(): Other transactions have committed. " <<
                        " My ts: " << trans->l_commit_ts << " Conflicting ts: " << end_trans_t_2->l_commit_ts << 
                        ". Conflicting rows: " << row_t << endl;
                    #endif
                    return -1;
                }
            }
        }
        end_trans_t = end_trans_t_2;
        end_trans_t_2 = __atomic_load_n(&end_trans_t->next, MM_ACQUIRE);
    }
    // No conflict such that move l_end_trans forward.
    __atomic_store_n(&trans->l_end_trans, end_trans_t, MM_CST);

    return 0;
}

int Rabit::pos2RE(int start, int end, Btv_set &pos_re)
{
    for (int i = start; i <= end; i++)
        pos_re.insert((uint32_t)i);

    return 0;
}

int Rabit::pos2GE(int from, int to, Btv_set &pos_ge)
{
    if(from == FROM_INV) {
        pos_ge.insert((uint32_t)to);
        auto group = get_group_id((uint32_t)to);
        pos_ge.insert((uint32_t)group);
    }
    else {
        pos_ge.insert((uint32_t)from);
        pos_ge.insert((uint32_t)to);
        auto group1 = get_group_id((uint32_t)from);
        auto group2 = get_group_id((uint32_t)to);
        if(group1 != group2) {
            pos_ge.insert((uint32_t)group1);
            pos_ge.insert((uint32_t)group2);
        }
    }
    return 0;
}

int Rabit::append(int tid, int val)
{
    return append(tid, val, UINT64_MAX);
}

int Rabit::append(int tid, int val, uint64_t row_id) 
{
    RABIT_ThreadInfo *th = &g_ths_info[tid];
    assert(val < num_btvs);

    TransDesc *trans = (TransDesc *)__atomic_load_n(&th->active_trans, MM_ACQUIRE);

    if (!trans)
        trans = trans_begin(tid);

    Btv_set pos_set{};
    if (config->encoding == EE) {
        pos_set.insert(val);
    }
    else if (config->encoding == RE) {
        pos2RE(val, num_btvs-1, pos_set);
    }
    else if (config->encoding == GE) {
        pos2GE(FROM_INV, val, pos_set);
    }

    // We temporarily set row_id values to prevent 
    // the rubs of a transactions' insert operations from overwriting each other.
    if(row_id != UINT64_MAX) {
        trans->rubs[row_id] = RUB{row_id, TYPE_INSERT, pos_set};
    }
    else {
        trans->rubs[trans->l_number_of_rows + trans->l_inc_rows] = 
            RUB{trans->l_number_of_rows + trans->l_inc_rows, TYPE_INSERT, pos_set};
    }
    trans->l_inc_rows ++;

    if (autoCommit) {
        return trans_commit(tid);
    }

    return 0;
}

int Rabit::update(int tid, uint64_t row_id, int to_val)
{
    RABIT_ThreadInfo *th = &g_ths_info[tid];
    assert(to_val < num_btvs);

    rcu_read_lock();

    TransDesc * trans = (TransDesc *)__atomic_load_n(&th->active_trans, MM_ACQUIRE);

    // rcu_read_lock() must be invoked before accessing g_timestamp in trans_begin(),
    // to guarantee that the corresponding btv cannot be reclaimed before
    // we access it in get_value_rcu().

    if (!trans)
        trans = trans_begin(tid);

    RUB last_rub = RUB{0, TYPE_INV, {}};
    int from_val = get_value_rcu(row_id, trans->l_start_ts, last_rub);
    assert(from_val < num_btvs);

    rcu_read_unlock();

    if (from_val == -1) {
        #if defined(VERIFY_RESULTS)
        cout << "NOTE in update(): the value at row " << row_id <<
            " has been deleted. I will return." << endl;
        #endif
        delete_trans(tid, trans);
        __atomic_store_n(&th->active_trans, NULL, MM_RELAXED);
        return -ENOENT;
    }
    else if (to_val == from_val) {
        #if defined(VERIFY_RESULTS)
        cout << "NOTE in update(): from_val == to_val (" << to_val <<
            "). I will return." << endl;
        #endif
        delete_trans(tid, trans);
        __atomic_store_n(&th->active_trans, NULL, MM_RELAXED);
        return -ENOENT;
    }

    Btv_set pos_set{};
    int min, max;
    if (from_val < to_val) {
        min = from_val;
        max = to_val - 1;
    } else {
        min = to_val;
        max = from_val - 1;
    }
    if (config->encoding == EE) {
        pos_set.insert(from_val);
        pos_set.insert(to_val);
    }
    else if (config->encoding == RE) {
        pos2RE(min, max, pos_set);
    }
    else if (config->encoding == GE) {
        pos2GE(from_val, to_val, pos_set);
    }

    trans->rubs[row_id] = RUB{row_id, TYPE_UPDATE, pos_set};

    if (autoCommit) {
        return trans_commit(tid);
    }

    return 0;
}

int Rabit::remove(int tid, uint64_t row_id)
{
    struct RABIT_ThreadInfo *th = &g_ths_info[tid];
    assert(row_id < g_number_of_rows);

    rcu_read_lock();

    struct TransDesc * trans = (TransDesc *)__atomic_load_n(&th->active_trans, MM_ACQUIRE);

    if (!trans)
        trans = trans_begin(tid);

    RUB last_rub = RUB{0, TYPE_INV, {}};
    int val = get_value_rcu(row_id, trans->l_start_ts, last_rub);
    assert(val < num_btvs);

    rcu_read_unlock();

    if (val == -1) {
        #if defined(VERIFY_RESULTS)
        cout << "Remove: The value at row " << row_id << " has been deleted." <<
            "I will return." << endl;
        #endif
        delete_trans(tid, trans);
        __atomic_store_n(&th->active_trans, NULL, MM_RELAXED);
        return -ENOENT;
    }

    Btv_set pos_set{};
    if (config->encoding == EE) {
        pos_set.insert(val);
    }
    else if (config->encoding == RE) {
        pos2RE(val, num_btvs-1, pos_set);
    }
    else if (config->encoding == GE) {
        pos2GE(FROM_INV, val, pos_set);
    }

    trans->rubs[row_id] = RUB{row_id, TYPE_DELETE, pos_set};

    if (autoCommit) {
        return trans_commit(tid);
    }

    return 0;
}

int Rabit::evaluate(int tid, uint32_t val)
{   
    RABIT_ThreadInfo *th = &g_ths_info[tid];
    assert(val < num_btvs);

    rcu_read_lock();

    TransDesc *trans = (TransDesc *)__atomic_load_n(&th->active_trans, MM_ACQUIRE);

    if (!trans)
        trans = trans_begin(tid);

    auto t1 = std::chrono::high_resolution_clock::now();

    Bitvector *bitvector_t = __atomic_load_n(&Btvs[val], MM_ACQUIRE);

    auto start_trans = bitvector_t->log_shortcut;

    uint64_t tsp_end = READ_ONCE(trans->l_start_ts);

    map<uint64_t, RUB> rubs{};

    get_rubs_on_btv(start_trans->l_commit_ts, tsp_end, start_trans, val, rubs);

    auto t2 = std::chrono::high_resolution_clock::now();

    uint64_t cnt = 0UL;
    #if defined(VERIFY_RESULTS)
        uint64_t cnt_test = 0UL;
    #endif

    SegBtv *seg_btv = READ_ONCE(bitvector_t->seg_btv);

    std::map<uint32_t, std::set<uint64_t>> seg_rids;
    for (const auto &[row_id_t, rub_t] : rubs) {
        assert(rub_t.pos.count(val));
        uint32_t idx = seg_btv->getSegId(row_id_t);
        seg_rids[idx].insert(row_id_t);
    }

    if (config->decode) {
        std::vector<uint32_t> dummy;
        seg_btv->decode(dummy, config);
        cnt = 0;
    } else {
        if (config->enable_parallel_cnt) {
            cnt = seg_btv->do_cnt_parallel_withtimestamp(config, seg_rids, start_trans->l_commit_ts);
            #if defined(VERIFY_RESULTS)
                cnt_test = old_seg_btv->do_cnt();
                assert(cnt == cnt_test);
            #endif
        }
        else {
            seg_btv->_cnt_in_thread_withtimestamp(&cnt, 0, seg_btv->seg_table.size(), start_trans->l_commit_ts, seg_rids, config);
        }
    }

    auto t3 = std::chrono::high_resolution_clock::now();

    // cout << "[Evaluate(ms)] [Non-Curve] [No merge]" <<
    //     "    Total: " << to_string(std::chrono::duration_cast<std::chrono::microseconds>(t4-t1).count()) <<
    //     "    Get btvs: " << to_string(std::chrono::duration_cast<std::chrono::microseconds>(t2-t1).count()) <<
    //     "    Count: " << to_string(std::chrono::duration_cast<std::chrono::microseconds>(t3-t2).count()) <<
    //     "    Process Rubs: " << to_string(std::chrono::duration_cast<std::chrono::microseconds>(t4-t3).count()) << endl;

    // This barrier can be moved forward.
    // However, performance improvement is negligibal since
    // the memory reclamation has been delegated to background threads.
    // We leave it here to make the code easy to understand.
    rcu_read_unlock();

    if (autoCommit) {
        auto t = trans_commit(tid);
        assert(t == -ENOENT);
    }

    return cnt;
}

int Rabit::_get_bit(uint32_t val, uint64_t row_id, uint64_t l_timestamp)
{
    TransDesc *start_trans = nullptr;
    int bit1 = 0;
    int bit2 = 0;
    RUB rub_t = RUB{0, TYPE_INV, {}};
    Bitvector *Btv = get_btv(val);
    start_trans = Btv->log_shortcut;

    /* Need to check RUBs in between (start_trans.ts, tsp_end]. */
    uint64_t tsp_end = READ_ONCE(l_timestamp);

    SegBtv *p = __atomic_load_n(&Btv->seg_btv, MM_ACQUIRE);
    btv_seg* curr_seg = p->GetSeg(row_id);
    start_trans = __atomic_load_n(&Btv->log_shortcut, MM_ACQUIRE);
    bit1 = curr_seg->btv->getBit(row_id - curr_seg->start_row, config);
    curr_seg->buffer->get_row_rub_from_buffer(row_id, val, start_trans->l_commit_ts, rub_t);

    uint64_t rub_tsp_t = 0UL;
    get_rub_on_row(start_trans->l_commit_ts, tsp_end, start_trans,
            row_id, rub_t, rub_tsp_t);

    if (rub_t.type != TYPE_INV && rub_t.pos.count(val))
        bit2 = 1;
    
    return bit1 ^ bit2;
}

void Rabit::_get_value(uint64_t row_id, int begin, int range, uint64_t l_timestamp,
        bool *flag, int *result, RUB *rub, uint64_t *rub_tsp)
{
    int ret = -1;

    if (config->encoding == EE || config->encoding == GE) {
        if (__atomic_load_n(flag, MM_CST)) 
            goto out;
    }

    for(int i = 0; i < range; i++) {
        uint32_t curVal = (uint32_t)begin + i;
        RUB rub_t = RUB{0, TYPE_INV, {}};
        uint64_t rub_tsp_t = 0UL;
        
        if (config->encoding == EE || config->encoding == GE) {
            if (__atomic_load_n(flag, MM_CST))
                break;
        }

        TransDesc *start_trans = nullptr;
        Bitvector *Btv = get_btv(curVal);
        start_trans = Btv->log_shortcut;
        uint64_t tsp_end = l_timestamp;

        int bit1 = 0;
        int bit2 = 0;

        SegBtv *p = __atomic_load_n(&Btv->seg_btv, MM_ACQUIRE);
        assert(p != nullptr);
        btv_seg* curr_seg = p->GetSeg(row_id);
        bit1 = curr_seg->btv->getBit(row_id - curr_seg->start_row, config);
        curr_seg->buffer->get_row_rub_from_buffer(row_id, curVal, start_trans->l_commit_ts, rub_t);

        get_rub_on_row(start_trans->l_commit_ts, tsp_end, start_trans, row_id, rub_t, rub_tsp_t);

        if (rub_t.type != TYPE_INV && rub_t.pos.count(curVal))
            bit2 = 1;

        // Record the rub value with the largest timestamp value.
        if (rub_t.type != TYPE_INV) {
            #if defined(VERIFY_RESULTS)
            if (rub_tsp_t == *rub_tsp) {
                assert(rub_t.pos == rub->pos);
            }
            #endif
            if (rub_tsp_t > *rub_tsp) {
                *rub = rub_t;
                *rub_tsp = rub_tsp_t;
            }
        }

        auto result = bit1 ^ bit2;

        if (result == 1) {
            if (config->encoding == EE || config->encoding == GE) {
                ret = curVal;
                __atomic_store_n(flag, true, MM_CST);
                break;
            }
            else if (config->encoding == RE) {
                if (ret == -1) {
                    ret = curVal;
                } else {
                    ret = (curVal < ret) ? curVal : ret;
                }
            }
        }
    }

out:
    __atomic_store_n(result, ret, MM_RELEASE);
}

int Rabit::get_value_rcu(uint64_t row_id, uint64_t l_timestamp, RUB &last_rub) 
{

    bool flag = false;
    int n_threads = (config->nThreads_for_getval > num_btvs) ? num_btvs : config->nThreads_for_getval;
    int begin = 0;
    int offset = num_btvs / n_threads;
    thread * getval_threads = new thread[n_threads];
    int * local_results = new int[n_threads];
    fill_n(local_results, n_threads, -2);
    RUB *last_rub_t = new RUB[n_threads]{};
    uint64_t *last_rub_tsp_t = new uint64_t[n_threads];
    fill_n(last_rub_tsp_t, n_threads, 0UL);
    uint64_t last_rub_tsp = 0UL;

    assert(offset >= 1); 

    for (int i = 0; i < n_threads; i++) {
        int begin = i * offset;
        int range = offset;
        if ((i == (n_threads-1)) && (num_btvs > n_threads))
            range += (num_btvs % n_threads);

        getval_threads[i] = thread(&Rabit::_get_value, this, row_id, begin, range, l_timestamp,
                                &flag, &local_results[i], &last_rub_t[i], &last_rub_tsp_t[i]);
    }

    int ret = -1; 
    for (int t = 0; t < n_threads; t++) {
        getval_threads[t].join();
        int tmp = __atomic_load_n(&local_results[t], MM_CST);
        if (tmp != -1) {
            if (config->encoding == EE || config->encoding == GE) {
                // assert(ret == -1);
                if(ret != -1) {
                    printTransQueue(l_timestamp);
                    std::cout << std::endl << "conflict row_id : " << row_id << " two ans : " << ret << " and " << tmp << std::endl;
                    std::cout << "conflict trans start_time : " << l_timestamp << std::endl << std::endl;
                    uint128_t data_t = __atomic_load_n((uint128_t *)&g_timestamp, MM_RELAXED);
                    printTransQueue(data_t);
                    assert(0);
                }
                ret = tmp;
            }
            else if (config->encoding == RE) {
                if (ret == -1) {
                    ret = tmp;
                } else {
                    ret = (tmp < ret) ? tmp : ret;
                }
            }
        }

        if (last_rub_t[t].type != TYPE_INV) {
            #if defined(VERIFY_RESULTS)
            if (last_rub_tsp_t[t] == last_rub_tsp) {
                assert(last_rub_t[t].pos == last_rub.pos);
            }
            #endif
            if (last_rub_tsp_t[t] > last_rub_tsp) {
                last_rub = last_rub_t[t];
                last_rub_tsp = last_rub_tsp_t[t];
            }
        }
    }

    delete[] local_results;
    delete[] getval_threads;
    delete[] last_rub_t;
    delete[] last_rub_tsp_t;

    return ret;
}

void Rabit::printTransQueue(uint64_t timestamp_t)
{
    lock_guard<shared_mutex> guard(g_debug_lk);
    auto trans = this->trans_queue->head->next;
    while (trans && trans->l_commit_ts <= timestamp_t)
    {
        std::cout << "trans start time : " << trans->l_start_ts << " trans commit time : " << trans->l_commit_ts << std::endl;
        for (const auto & [row_id_t, rub_t] : trans->rubs)
        {
            std::cout << "row_id : " << row_id_t << " type : " << rub_t.type << std::endl;
            for(const auto position : rub_t.pos)
            {
                std::cout << "row_id : " << row_id_t << " type : " << rub_t.type << " position : " << position << std::endl;
            }
        }
        std::cout << std::endl;
        trans = trans->next;
    }
}

void Rabit::printMemorySeg() {
    uint64_t bitmap = 0, updateable_bitmap = 0, fence_pointers = 0;
    for (int i = 0; i < num_btvs; ++i) {
        for (const auto & [id_t, seg_t] : Btvs[i]->seg_btv->seg_table) {
            bitmap += seg_t->btv->getSerialSize();
            fence_pointers += seg_t->btv->index.size() * sizeof(int) * 2;
        }
    }
    std::cout << "Seg M FP " << fence_pointers << std::endl;
    std::cout << "Seg M BM " << bitmap << std::endl;
}

void Rabit::printUncompMemorySeg() {
    uint64_t bitmap = 0, fence_pointers = 0;
    for (int i = 0; i < num_btvs; ++i) {
        for (const auto & [id_t, seg_t] : Btvs[i]->seg_btv->seg_table) {
            seg_t->btv->appendActive();
            seg_t->btv->decompress();
            bitmap += seg_t->btv->getSerialSize();
            seg_t->btv->compress();
            fence_pointers += seg_t->btv->index.size() * sizeof(int) * 2;
        }
    }
    std::cout << "Seg UncM FP " << fence_pointers << std::endl;
    std::cout << "Seg UncM BM " << bitmap << std::endl;
}

uint64_t Rabit::range(int tid, uint32_t start, uint32_t range)
{
    uint64_t cnt = 0;
    SegBtv *res = range_res(tid, start, range);
    res->_count_ones_in_thread(&cnt, 0, res->seg_table.size());
    delete res;
    return cnt;
}

SegBtv *Rabit::range_res(int tid, uint32_t start, uint32_t range)
{
    RABIT_ThreadInfo *th = &g_ths_info[tid];

    rcu_read_lock();

    TransDesc *trans = (TransDesc *)__atomic_load_n(&th->active_trans, MM_ACQUIRE);

    if (!trans)
        trans = trans_begin(tid);

    SegBtv* res = new SegBtv(*Btvs[start]->seg_btv);
    res->decompress();

    std::vector<std::pair<uint32_t, uint32_t>> range_vec;

    if(config->encoding == EE) {
        range_vec.emplace_back(std::pair(start, start + range - 1));
    }
    else if(config->encoding == GE) 
    {
        uint32_t end = start + range - 1;
        auto group_start = get_group_id(start);
        auto group_end = get_group_id(end);

        if ((start % config->GE_group_len == 0) && 
            (end % config->GE_group_len == config->GE_group_len - 1)) {
            range_vec.emplace_back(std::pair(group_start, group_end));
        }
        else if (group_start == group_end) {
            range_vec.emplace_back(std::pair(start, end));
        } else {
            auto group_len = config->GE_group_len;

            auto last_EE_in_first_group = (start/group_len*group_len) + group_len - 1;
            auto first_EE_in_first_group = start/group_len*group_len;
            if(first_EE_in_first_group == start) {
                range_vec.emplace_back(std::pair(group_start, group_start));
            }
            else {
                range_vec.emplace_back(std::pair(start, last_EE_in_first_group));
            }

            range_vec.emplace_back(std::pair(group_start+1, group_end-1));

            auto last_EE_in_last_group = (end/group_len*group_len) + group_len - 1;
            auto first_EE_in_last_group = end/group_len*group_len;
            if(last_EE_in_last_group == end) {
                range_vec.emplace_back(std::pair(group_end, group_end));
            }
            else {
                range_vec.emplace_back(std::pair(first_EE_in_last_group, end));
            }
        }
    }
    else {
        std::cout << "not implemented" << std::endl;
        exit(-1);
    }

    for(auto &p : range_vec) {
        for(auto i = p.first; i <= p.second; i++) {
            _or_btv(*res, i, trans);
        }
    }

    rcu_read_unlock();
    if (autoCommit) {
        auto t = trans_commit(tid);
        assert(t == -ENOENT);
    }

    return res;
}

SegBtv *Rabit::range_or_GE(int tid, uint32_t start, uint32_t range) {
    RABIT_ThreadInfo *th = &g_ths_info[tid];

    rcu_read_lock();

    TransDesc *trans = (TransDesc *)__atomic_load_n(&th->active_trans, MM_ACQUIRE);

    if (!trans)
        trans = trans_begin(tid);

    SegBtv* res = new SegBtv(*Btvs_GE[start]->seg_btv);
    res->decompress();

    std::vector<std::pair<uint32_t, uint32_t>> range_vec;

    range_vec.emplace_back(std::pair(start, start + range - 1));

    for(auto &p : range_vec) {
        for(auto i = p.first; i <= p.second; i++) {
            _or_btv_GE(*res, i, trans);
        }
    }

    rcu_read_unlock();
    if (autoCommit) {
        auto t = trans_commit(tid);
        assert(t == -ENOENT);
    }

    return res;
}

void Rabit::_or_btv(SegBtv &res, uint32_t idx, TransDesc *trans) 
{
    auto tsp_end = READ_ONCE(trans->l_start_ts);
    Bitvector *Btv = get_btv(idx);
    auto start_trans = Btv->log_shortcut;
    map<uint64_t, RUB> rubs{};
    std::map<uint32_t, std::set<uint64_t>> seg_rids;
    
    get_rubs_on_btv(start_trans->l_commit_ts, tsp_end, start_trans, idx, rubs);
    
    for (const auto &[row_id_t, rub_t] : rubs) {
        assert(rub_t.pos.count(idx));
        uint32_t idx = res.getSegId(row_id_t);
        seg_rids[idx].insert(row_id_t);
    }
    res._or_in_thread_with_timestamp(*Btv->seg_btv, 0, res.seg_table.size(), start_trans->l_commit_ts, seg_rids, config);
}

void Rabit::_or_btv_GE(SegBtv &res, uint32_t idx, TransDesc *trans) 
{
    auto tsp_end = READ_ONCE(trans->l_start_ts);
    Bitvector *Btv = Btvs_GE[idx];
    auto start_trans = Btv->log_shortcut;
    map<uint64_t, RUB> rubs{};
    std::map<uint32_t, std::set<uint64_t>> seg_rids;
    
    get_rubs_on_btv(start_trans->l_commit_ts, tsp_end, start_trans, idx, rubs);
    
    for (const auto &[row_id_t, rub_t] : rubs) {
        assert(rub_t.pos.count(idx));
        uint32_t idx = res.getSegId(row_id_t);
        seg_rids[idx].insert(row_id_t);
    }
    res._or_in_thread_with_timestamp(*Btv->seg_btv, 0, res.seg_table.size(), start_trans->l_commit_ts, seg_rids, config);
}

// This is a helper function
// which allows external applications (e.g., DBx1000 and PostgreSQL) to initialize the bitmap index.
// This function shouldn't be used as usual.
int Rabit::__init_append(int tid, int rowID, int val) 
{
    static mutex g_lock;

    lock_guard<mutex> guard(g_lock);
    if (config->on_disk) { }
    else {

        if (rowID >= g_number_of_rows) {
            g_number_of_rows = rowID + 1;

            for (int i = 0; i < num_btvs; ++i) {
                Btvs[i]->seg_btv->adjustSize(0, g_number_of_rows);
            }
        }

        Btvs[val]->seg_btv->setBit(rowID, 1, config);

        // if (config->enable_fence_pointer) {
        //     bitmaps[val]->btv->index.clear();
        //     bitmaps[val]->btv->buildIndex();
        // }
    }

    return 0;
} 

TransDesc * TransDesc::get_rubs_on_btv(uint64_t tsp_begin, uint64_t tsp_end, 
                        TransDesc *trans, uint32_t val, std::map<uint64_t, RUB> &rubs)
{
    assert(tsp_begin <= tsp_end);
    assert(trans);

    // Check trans in between (tsp_begin, tsp_end].
    TransDesc *trans_prev = trans;
    trans = READ_ONCE(trans->next);
    while (__atomic_load_n(&trans, MM_ACQUIRE) && (READ_ONCE(trans->l_commit_ts) <= tsp_end)) {
        if (READ_ONCE(trans->l_commit_ts) > tsp_begin) 
        {   // The normal case
            for (const auto & [row_id_t, rub_t] : trans->rubs)
            {
                if (rub_t.pos.count(val)) {
                    // perv trans just change once
                    if(rubs.count(row_id_t) && rubs[row_id_t].pos.count(val))
                        rubs.erase(row_id_t);
                    else
                    {
                        rubs[row_id_t] = RUB{row_id_t, rub_t.type, Btv_set{val}};
                    }
                }
            }
        } else if (READ_ONCE(trans->l_commit_ts) == tsp_begin) {
            ; // Skip the first trans
        } else {
            assert(false);
        }

        trans_prev = trans;
        trans = READ_ONCE(trans->next);
    }

    return trans ? trans : trans_prev;
}

TransDesc * TransDesc::get_rubs(TransDesc *trans_end, std::map<uint64_t, RUB> &rubs)
{
    auto trans_itor = this;
    while(1) {
        for (const auto & [row_id_t, rub_t] : trans_itor->rubs)
        {
            if(!rubs.count(row_id_t))
            {
                rubs[row_id_t] = RUB();
                rubs[row_id_t].type = TYPE_MERGE;
                rubs[row_id_t].row_id = row_id_t;
            }

            for(const auto position : rub_t.pos)
            {
                if(rubs[row_id_t].pos.count(position))
                    rubs[row_id_t].pos.erase(position); // ????
                else
                {
                    rubs[row_id_t].pos.insert(position);
                }
            }
            if(rubs[row_id_t].pos.empty())
                rubs.erase(row_id_t);
        }
        if(trans_itor == trans_end)
            break;
        trans_itor = trans_itor->next;
    }
    return trans_itor;
}

void Rabit::merge_worker(pos_segs& pos_seg, TransDesc* worker_trans_start,
                    TransDesc* worker_trans_end , uint32_t begin, uint32_t end)
{
    for(auto btv_idx = begin; btv_idx != end; ++btv_idx)
    {
        auto Btv = get_btv(btv_idx);
        //replace bitvector
        for(auto seg_id : Btv->replace_list) {
            Btv->seg_btv->Replacesegbtv(seg_id, Btv->log_shortcut->l_commit_ts, config);
        }
        Btv->replace_list.clear();

        // write buffer
        auto s = pos_seg.find(btv_idx);
        if(s != pos_seg.end()) {
            write_rids_to_buffer(s->second, btv_idx, worker_trans_end->l_commit_ts);
        }

        // update start trans
        __atomic_store_n(&Btv->log_shortcut, worker_trans_end, MM_RELEASE);
    }

}

void Rabit::write_rids_to_buffer(seg_rids& s, uint32_t btv_idx, uint64_t l_timestamp)
{
    auto Btv = get_btv(btv_idx);
    // write buffer based on segment
    for(auto const &[seg_id, rids] : s) {
        auto it = Btv->seg_btv->seg_table.find(seg_id);

        // need to insert new seg_id
        assert(it != Btv->seg_btv->seg_table.end());

        btv_seg* curr_seg = it->second;
        auto buffer_pos = curr_seg->buffer->add_rids(l_timestamp, rids);
        if(buffer_pos > BUFFER_MERGE_SIZE) {
            Btv->replace_list.insert(seg_id);
        }
    }
}

Bitvector * Rabit::get_btv(uint32_t idx) 
{
    return (idx >= GE_IDX_BEGIN) ? Btvs_GE[idx - GE_IDX_BEGIN] : Btvs[idx];
}

bool run_merge_func = false;
#define SLEEP_WHEN_IDEL (10000)
#define MERGE_THRESHOLD (8)

void rabit_merge_dispatcher(BaseTable *table)
{
    assert(table->config->approach == "rabit");

    // rcu_register_thread();

    rabit::Rabit* table2 = dynamic_cast<rabit::Rabit*>(table);

    while (READ_ONCE(run_merge_func)) {
        rabit_merge(table2);
    }


    // rcu_quiescent_state();
    // rcu_unregister_thread();
}

void rabit_merge(rabit::Rabit *table)
{
    // Read the tail of Delta Log.
    auto worker_trans_start = table->get_merge_start_trans();
    auto worker_trans_end = table->get_merge_end_trans();

    if(!worker_trans_start) {
        this_thread::sleep_for(chrono::microseconds(SLEEP_WHEN_IDEL));
        return;
    }

    // ???? 
    if(worker_trans_end->l_commit_ts < worker_trans_start->l_commit_ts)
        worker_trans_end = worker_trans_start;

    int merge_count = 1;
    TransDesc *trans_itor = worker_trans_start;
    while(trans_itor != worker_trans_end) {
        trans_itor = trans_itor->next;
        if(++merge_count == MERGE_THRESHOLD) {
            worker_trans_end = trans_itor;
            break;
        }
    }

    Rabit::pos_segs pos_seg;

    // Get rubs
    std::map<uint64_t, RUB> rubs;
    worker_trans_start->get_rubs(worker_trans_end, rubs);

    // Find <row_id, position>
    for (const auto & [row_id_t, rub_t] : rubs)
    {
        for(const auto btv_idx : rub_t.pos)
        {
            pos_seg[btv_idx][table->get_btv(btv_idx)->seg_btv->getSegId(row_id_t)].insert(row_id_t);
        }
    }

    uint32_t idx_size = table->num_btvs + table->num_btvs_GE;
    uint32_t n_threads = table->config->nThreads_for_merge;
    uint32_t n_btv_per_thread = idx_size / n_threads;
    uint32_t reminder = idx_size % n_threads;
    thread* threads = new thread[n_threads];

    assert(n_btv_per_thread >= 1);

    vector<uint32_t>begin (n_threads + 1, 0); 
    for(uint32_t i = 1; i <= reminder; i++)
        begin[i] = begin[i - 1] + n_btv_per_thread + 1;
    for(uint32_t i = reminder + 1; i <= n_threads; i++)
        begin[i] = begin[i - 1] + n_btv_per_thread;

    synchronize_rcu();

    for (int i = 0; i < n_threads; i++) {
        threads[i] = std::thread(&rabit::Rabit::merge_worker, table, std::ref(pos_seg), worker_trans_start, worker_trans_end, begin[i], begin[i+1]);
    }

    for (int t = 0; t < n_threads; t++) {
        threads[t].join();
    }
    delete[] threads;

    table->set_merge_cursor(worker_trans_end);

}
