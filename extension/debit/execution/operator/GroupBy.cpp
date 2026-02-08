#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution/tpch/bitmap_groupby.hpp"
#include "bitmaps/rabit/table.h"

#include <iostream>
#include <chrono>

namespace duckdb {
void BMGroupBy::BM_PerfectHashGroupBy(ExecutionContext &context, vector<string> &group_colnames) {
    if (*cursor == 0) {
        vector<rabit::Rabit*> rabit_ptrs;
        for (auto &col_name : group_colnames) {
            rabit::Rabit *ptr = nullptr;
            if (col_name == "l_returnflag") {
                ptr = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
            } else if (col_name == "l_linestatus") {
                ptr = dynamic_cast<rabit::Rabit *>(context.client.bitmap_linestatus);
            }
            if (ptr) {
                rabit_ptrs.push_back(ptr);
            }
        }
        if (rabit_ptrs.size() == 1) {
            for (int i = 0; i < rabit_ptrs[0]->config->g_cardinality; i++) {
                ibis::bitvector btv_group;
                btv_group.copy(*rabit_ptrs[0]->Btvs[i]->btv);
                btv_group.decompress();
                if (context.client.btv_last_pipeline)  {
                    btv_group &= *(context.client.btv_last_pipeline);
                }
                context.client.bitmap_vector_pool.push_back(btv_group);
            }
        }
        else if (rabit_ptrs.size() == 2) {
            for (int i = 0; i < rabit_ptrs[0]->config->g_cardinality; i++) {
                for (int j = 0; j < rabit_ptrs[1]->config->g_cardinality; j++) {
                    ibis::bitvector btv_group;
                    btv_group.copy(*rabit_ptrs[0]->Btvs[i]->btv);
                    btv_group.decompress();
                    ibis::bitvector ttt_res;
                    ttt_res.copy(*rabit_ptrs[1]->Btvs[j]->btv);
                    ttt_res.decompress();
                    btv_group &= ttt_res;
                    if (!btv_group.count()) continue;
                    if (context.client.btv_last_pipeline)  {
                        btv_group &= *(context.client.btv_last_pipeline);
                    }
                    std::cout << "GroupBy bitmap rows: " << btv_group.count() << std::endl;
                    context.client.bitmap_vector_pool.push_back(btv_group);
                }
            }
        }

        *cursor = 1;
    }
    else {
        return;
    }
    
}

}