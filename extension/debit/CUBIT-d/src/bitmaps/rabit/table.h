#ifndef RABIT_TABLE_H_
#define RABIT_TABLE_H_

#include <vector>
#include <mutex>
#include <cstdint>
#include <atomic>
#include <tuple>
#include <urcu.h>
#include <unordered_set>

#include "utils/util.h"
#include "bitmaps/base_table.h"
#include "bitmaps/rabit/segBtv.h"

void rabit_merge_dispatcher(BaseTable *table);
extern bool run_merge_func;

#define GE_IDX_BEGIN (num_btvs)


namespace rabit {

struct TransDesc 
{
    TransDesc *next;

    uint64_t l_number_of_rows;
    uint32_t l_inc_rows;

    uint64_t l_start_ts;
    uint64_t l_commit_ts;

    std::map<uint64_t, RUB> rubs;

    TransDesc *l_end_trans;

    uint64_t pos;

    TransDesc * get_rubs_on_btv(uint64_t tsp_begin, uint64_t tsp_end, 
                        TransDesc *trans, uint32_t val, std::map<uint64_t, RUB> &rubs);
    // Get rubs in the of range [this, trans_end]
    TransDesc * get_rubs(TransDesc *trans_end, std::map<uint64_t, RUB> &rubs);
} DOUBLE_CACHE_ALIGNED;

const int RABIT_PAD_BITS = 0;

#define FROM_INV (-1)
#define INV_TIMESTAMP (numeric_limits<uint64_t>::max())

struct Bitvector {
    struct rcu_head head;
    TransDesc * log_shortcut;
    ibis::bitvector *btv;
    SegBtv *seg_btv;
    Bitvector *next;
    std::set<uint32_t> replace_list;
    ~Bitvector() {if(next) delete next; if(btv) delete btv; if(seg_btv) delete seg_btv;}
} DOUBLE_CACHE_ALIGNED;

static inline
void free_bitvector_cb(struct rcu_head *head)
{
    struct Bitvector *Btv = caa_container_of(head, struct Bitvector, head);
    if (Btv->seg_btv)
        delete Btv->seg_btv;
    delete Btv;
}

typedef struct _queue_t {
    struct TransDesc * head DOUBLE_CACHE_ALIGNED;
    struct TransDesc * tail DOUBLE_CACHE_ALIGNED;
} queue_t DOUBLE_CACHE_ALIGNED;

class Rabit : public BaseTable {

private:
    /* *********************** */
    /* ** pool for TransDesc** */
    /* *********************** */
    TransDesc *trans_pool;
    uint64_t n_trans_pool;
    uint64_t cnt_trans_used;

    uint32_t total_rows;

    // Protect lkqueue, g_timestamp, g_number_of_rows, and bitmaps, all together.
    std::shared_mutex g_trans_lk;
    std::shared_mutex g_merge_lk_;
    TransDesc *merge_cursor_;

protected:

    /* *********************** */
    queue_t * trans_queue;

    /* Configure parameters */
    bool autoCommit;
    bool db_control;
    int merge_threshold;

    void _get_value(uint64_t, int, int, uint64_t, bool *, int *, struct RUB *, uint64_t *);

    int _get_bit(uint32_t val, uint64_t row_id, uint64_t l_timestamp);
    TransDesc * get_rub_on_row(uint64_t, uint64_t, TransDesc *, uint64_t, RUB &, uint64_t &);
    TransDesc * get_rubs_on_btv(uint64_t, uint64_t, TransDesc *, uint32_t, std::map<uint64_t, RUB> &);
    void get_rub_on_row_from_buffer(uint64_t row_id, Bitvector *Btv, uint64_t l_timestamp, RUB &rub);
    void get_rub_on_btv_from_buffer(uint16_t val, Bitvector *Btv, uint64_t l_timestamp, std::map<uint64_t, RUB> &rubs);

    TransDesc * allocate_trans();
    int delete_trans(int, TransDesc *);

    int check_conflicts(TransDesc *, TransDesc *);

    int pos2RE(int start, int end, Btv_set &pos_set);

    int pos2GE(int start, int end, Btv_set &pos_set);

    inline int get_group_id(uint32_t idx) {
        assert(idx < GE_IDX_BEGIN);
        return GE_IDX_BEGIN + (idx / config->GE_group_len);
    }

public:

    /* *********************** */
    struct Bitvector **Btvs;
    struct Bitvector **Btvs_GE;
    int num_btvs;
    int num_btvs_GE;

    // FIXME: can be moved to private domain in the future.
    uint64_t g_timestamp __attribute__((aligned(128)));
    uint64_t g_number_of_rows;
    std::shared_mutex g_debug_lk;

    // For executor plan
    // TODO: can support GE in the future
    int min_value_EE;
    int max_value_EE;

    typedef std::unordered_map<uint32_t, std::set<uint64_t> > seg_rids;
    typedef std::unordered_map<uint32_t, seg_rids> pos_segs;

    Rabit(Table_config *);
    ~Rabit() { int e=config->g_cardinality-(config->encoding != EE); for(int i = 0; i < e; i++) {delete Btvs[i];} }

    int append(int, int, uint64_t row_id);
    int append(int, int);
    int remove(int, uint64_t);
    int update(int, uint64_t, int);
    int evaluate(int, uint32_t);
    void _load_btv(int, int, TransDesc*, Table_config*);
    void _load_zipbtv(int, int, TransDesc*, Table_config*); 
    void _load_btv_GE(int, int, TransDesc*, Table_config*);
    void _or_btv(SegBtv &res, uint32_t idx, TransDesc *trans);
    void _or_btv_GE(SegBtv &res, uint32_t idx, TransDesc *trans);

    TransDesc * trans_begin(int, uint64_t db_timestamp_t = UINT64_MAX);
    int trans_commit(int tid, uint64_t db_timestamp_t = UINT64_MAX, uint64_t db_row_nums = UINT64_MAX);
    int merge_bitmap(int, uint32_t, TransDesc *, Bitvector *, Bitvector *, std::map<uint64_t, RUB> *);

    void printTransQueue(uint64_t timestamp_t);
    void printMemorySeg();
    void printUncompMemorySeg();

    int get_value_rcu(uint64_t, uint64_t, RUB &); 
    uint64_t range(int, uint32_t, uint32_t);
    SegBtv *range_res(int, uint32_t, uint32_t);
    SegBtv *range_or_GE(int, uint32_t, uint32_t);
    uint64_t get_g_timestamp() { return g_timestamp; }
    uint64_t get_g_number_of_rows() { return g_number_of_rows; }
    TransDesc *GetTransPool() { return trans_pool; }
    bool init_sequential_test();
    Bitvector * get_btv(uint32_t idx);

    void merge_worker(pos_segs& pos_seg_idx, TransDesc* worker_trans_start, 
            TransDesc* worker_trans_end, uint32_t begin, uint32_t end);
    void write_rids_to_buffer(seg_rids& s, uint32_t idx, uint64_t l_timestamp);

    TransDesc * get_merge_start_trans() { return merge_cursor_->next; }
    TransDesc * get_merge_end_trans() { return trans_queue->tail; }
    void set_merge_cursor(TransDesc *trans) { merge_cursor_ = trans;}

    int __init_append(int tid, int rowID, int val);
};

};

void rabit_merge(rabit::Rabit *table);
#endif
