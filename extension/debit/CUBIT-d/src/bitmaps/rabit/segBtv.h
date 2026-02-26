#ifndef SEG_BTV_H_
#define SEG_BTV_H_

#include <map>

#include "fastbit/bitvector.h"

#define BTV_BUFFER_SIZE 50
#define BUFFER_MERGE_SIZE 40
#define UN_USED_SOLT (((uint128_t)1 << 127) - 1)

// FIXME: The following two definitions should be moved to table.h
typedef std::unordered_set<uint32_t> Btv_set;

// TODO: We have changed the name to HUD in the paper.
struct RUB {
    uint64_t row_id;
    int type;
    Btv_set pos;
};

struct btv_buffer 
{
    uint128_t buffer_[BTV_BUFFER_SIZE];
    uint64_t buffer_pos_;
    
    btv_buffer() {
        buffer_pos_ = 0;
        for(int i = 0; i < BTV_BUFFER_SIZE; i++) {
            buffer_[i] = UN_USED_SOLT;
        }
    }

    void get_row_rub_from_buffer(uint64_t row_id, uint32_t position, uint64_t l_timestamp, RUB &rub) {
        bool count = false;
        for(size_t i = 0; i < BTV_BUFFER_SIZE; i++) {
            uint128_t data_t = __atomic_load_n((uint128_t *)&buffer_[i], MM_ACQUIRE);
            if( (uint64_t)(data_t>>64) > l_timestamp) {
                break;
            } else {
                if( (uint64_t)data_t == row_id ) {
                    count = !count;
                }
            }
        }

        if( count ) {
            if(rub.pos.count(position)) {
                rub.pos.erase(position);
            } else{
                rub.row_id = row_id;
                rub.type = TYPE_MERGE;
                rub.pos.insert(position);
            }
        }
    }

    void get_rids_from_buffer(uint64_t l_timestamp, std::set<uint64_t> &row_ids) {
        for(size_t i = 0; i < BTV_BUFFER_SIZE; i++) {
            uint128_t data_t = __atomic_load_n((uint128_t *)&buffer_[i], MM_ACQUIRE);
            if((uint64_t)(data_t>>64) > l_timestamp) {
                break;
            }
            else {
                if(row_ids.count((uint64_t)data_t))
                    row_ids.erase((uint64_t)data_t);
                else row_ids.insert((uint64_t)data_t);
            }
        }
    }

    uint64_t add_rids(uint64_t l_timestamp, const std::set<uint64_t>& row_ids) {
        for(auto row_id : row_ids) {
            __atomic_store_n(&buffer_[buffer_pos_++], 
                             ((uint128_t)l_timestamp << 64) + row_id, 
                             MM_CST);
        }
        return buffer_pos_;
    }
};

struct btv_seg {
    int id;
    uint64_t start_row, end_row;
    ibis::bitvector *btv;

    btv_buffer * buffer;
    btv_seg *next;
    
    btv_seg() : id(-1), start_row(0), end_row(0) 
                {btv = new ibis::bitvector(); buffer = new btv_buffer(); next = nullptr;};
    btv_seg(const btv_seg &rhs) : id(rhs.id), start_row(rhs.start_row), end_row(rhs.end_row)
                {btv = new ibis::bitvector(); btv->adjustSize(0, rhs.btv->size()); buffer = new btv_buffer(); };
    ~btv_seg() {if(btv) delete btv; if(buffer) delete buffer;};
    void setbit(uint64_t row_id,int val, Table_config *config) 
                {assert(row_id <= end_row); assert(row_id >= start_row); btv->setBit(row_id - start_row, val, config);};
};

class SegBtv {
private:
    uint32_t encoded_word_len;
    uint32_t num_segs;
    uint64_t rows_per_seg;
    uint64_t words_per_seg;
    uint64_t n_rows;

    btv_seg* getSeg(uint64_t);

    // This function verifies the correctness of the newly-built segmented btv.
    // It runs very very slow.
    int Verify(Table_config *, ibis::bitvector * const);

public:
    std::map<uint32_t, btv_seg *> seg_table{};

    SegBtv(Table_config *, ibis::bitvector *);
    SegBtv(const SegBtv &rhs);
    SegBtv(const SegBtv &rhs, int begin, int end);
    ~SegBtv();

    int deepCopy(const SegBtv &rhs);
    int rangeDeepCopy(const SegBtv &rhs, int begin, int end);
    void decompress();

    int getBit(uint64_t, Table_config *);
    void setBit(uint64_t, int, Table_config *);

    uint64_t do_cnt();
    uint64_t do_cnt_parallel(Table_config *);

    void _and_parallel(SegBtv &rhs, Table_config *);
    void _and_in_thread(SegBtv *rhs, int , int);

    void _cnt_in_thread(uint64_t* , int , int);
    void _count_ones_in_thread(uint64_t* , int , int);

    uint64_t do_cnt_parallel_withtimestamp(Table_config *config, const std::map<uint32_t, std::set<uint64_t>>& seg_rids, uint64_t l_timestamp);
    void _cnt_in_thread_withtimestamp(uint64_t *cnt, int begin, int end, uint64_t l_timestamp, const std::map<uint32_t, std::set<uint64_t>>& seg_rids, Table_config *config);
    uint64_t get_rows() {return n_rows;};
    int adjustSize(uint64_t, uint64_t);

    int buildIndex(uint64_t);
    int buildAllIndex();

    ///Perform bitwise exclusive or (XOR).
    void operator^=(SegBtv &rhs);

    void _or_in_thread_with_timestamp(SegBtv &rhs, uint32_t begin, uint32_t end, uint64_t l_timestamp, const std::map<uint32_t, std::set<uint64_t>>& seg_rids, Table_config *config);

    ///Perform bitwise XOR and return the result as a new bitvector.
    SegBtv *operator^(SegBtv &rhs);

    std::vector<uint32_t> decode(std::vector<uint32_t> &append_rowids, Table_config *config);
    
    btv_seg* GetSeg(uint64_t row_id) {return getSeg(row_id);};
    void Replacesegbtv(uint32_t seg_num, uint64_t l_timestamp, Table_config *config);
    uint32_t getSegId(uint64_t row_id) { return row_id / rows_per_seg; };
    uint64_t getRowsPerSeg() { return rows_per_seg; };
};

#endif



