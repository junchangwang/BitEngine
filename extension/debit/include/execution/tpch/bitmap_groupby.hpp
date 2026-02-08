#pragma once

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/typedefs.hpp"
#include "bitmaps/rabit/segBtv.h"

#include <cstdint>
#include <immintrin.h>
#include <netinet/in.h>

namespace duckdb {

class BMGroupBy {
public:
    BMGroupBy();

    ~BMGroupBy();

    vector<row_t> *row_ids;

    idx_t *cursor;

    idx_t num_idlist;

public:
    void BM_PerfectHashGroupBy(ExecutionContext &context, vector<string> &group_colnames);



};

}