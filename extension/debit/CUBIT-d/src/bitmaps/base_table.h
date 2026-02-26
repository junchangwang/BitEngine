#ifndef BASE_TABLE_H
#define BASE_TABLE_H

#include <thread>
#include <future>
#include <shared_mutex>

#include "utils/util.h"
#include "fastbit/bitvector.h"

extern uint64_t db_timestamp __attribute__((aligned(128)));
extern uint64_t db_number_of_rows;

class BaseTable {
public:
    BaseTable(Table_config *config) : config(config), cardinality(config->g_cardinality) {}

    Table_config *const config;

    virtual int update(int tid, uint64_t rowid, int to_val) { return -1; }

    virtual int remove(int tid, uint64_t rowid) { return -1; }

    virtual int append(int tid, int val) { return -1; }

    virtual int evaluate(int tid, uint32_t val) { return -1; }

    virtual uint64_t range(int tid, uint32_t start, uint32_t range) { return -1; }

    virtual void printMemory() { return; }

    virtual void printUncompMemory() { return; }

protected:
    const int32_t cardinality;

    std::string getBtvName(int val) {
        std::stringstream ss;
        //ss << INDEX_PATH << val << ".bm";
        ss << config->INDEX_PATH << "/" << val << "."<<config->file_format;
        return ss.str();
    }

    std::string getGroupName(int val) {
        std::stringstream ss;
        //ss << INDEX_PATH << val << ".bm";
        ss << config->GROUP_PATH << "/" << val * config->GE_group_len << ".bm";
        return ss.str();
    }
};

#endif
