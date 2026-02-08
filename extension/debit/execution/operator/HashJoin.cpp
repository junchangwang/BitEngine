#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution/tpch/bitmap_hash_join.hpp"
#include "bitmaps/rabit/table.h"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/common/types/value_map.hpp"

#include <iostream>
#include <chrono>

namespace duckdb {


OperatorResultType BMHashJoin::probeBitmap(ExecutionContext &context, JoinHashTable &build_ht, const PhysicalHashJoin &op, const string &probe_col_name) {
    if (context.client.bitmap_probe_done.find(probe_col_name) == context.client.bitmap_probe_done.end()) {

        context.client.bitmap_probe_done.insert(probe_col_name);

        int build_key_idx = -1;
        for (int j = 0; j < op.conditions.size(); ++j) {
            auto &cond = op.conditions[j];
            if (!cond.left) continue;
            if (cond.left->GetName() == probe_col_name) {
                build_key_idx = j;
                break;
            }
        }

        auto &data_collection = build_ht.GetDataCollection();

        Vector tuples_addresses(LogicalType::POINTER, build_ht.Count());
        JoinHTScanState join_ht_state(data_collection, 0, data_collection.ChunkCount(),
                                    TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
        idx_t key_count = build_ht.FillWithHTOffsets(join_ht_state, tuples_addresses);

        Vector build_vector(build_ht.layout_ptr->GetTypes()[build_key_idx], key_count);
        data_collection.Gather(tuples_addresses, *FlatVector::IncrementalSelectionVector(), key_count, build_key_idx,
                            build_vector, *FlatVector::IncrementalSelectionVector(), nullptr);

        value_set_t unique_ht_values;
        for (idx_t k = 0; k < key_count; k++) {
            unique_ht_values.insert(build_vector.GetValue(k));
        }
        vector<Value> in_list(unique_ht_values.begin(), unique_ht_values.end());
        

        rabit::Rabit *ptr = nullptr;
        std::vector<int64_t> *dst = nullptr;
        if (probe_col_name == "l_orderkey") {
            ptr = dynamic_cast<rabit::Rabit *>(context.client.bitmap_orderkey);

        } else if (probe_col_name == "l_suppkey") {
            ptr = dynamic_cast<rabit::Rabit *>(context.client.bitmap_suppkey);
        }

        ibis::bitvector btv_join;

        for (idx_t i = 0; i < key_count; ++i) {
            auto val = in_list[i].GetValue<int64_t>();
            if (i == 0) {
                btv_join.copy(*ptr->Btvs[val]->btv);
                btv_join.decompress();
            } else {
                btv_join |= *ptr->Btvs[val]->btv;
            }
        }

        dst = nullptr;
        ptr = nullptr;

        if (!context.client.btv_last_pipeline) {
            context.client.btv_last_pipeline = std::make_unique<ibis::bitvector>(btv_join);
        } else {
            *context.client.btv_last_pipeline &= btv_join;
        }

        std::cout << "rows: " << (*context.client.btv_last_pipeline).count() << std::endl;
        return OperatorResultType::NEED_MORE_INPUT;
    }
    else {
        return OperatorResultType::FINISHED;
    }
    
}




}