#include "execution/tpch/bitmap_groupby.hpp"

namespace duckdb {

BMGroupBy::BMGroupBy() 
{
    cursor = new idx_t(0);
    row_ids = new vector<row_t>;
}

BMGroupBy::~BMGroupBy() 
{
    delete cursor;
    delete row_ids;
}

}