#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "execution/tpch/bitmap_table_scan.hpp"
#include "bitmaps/rabit/table.h"
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

#include <iostream>
#include <chrono>

namespace duckdb {

template <class T>
void FilterSelectionSwitch(T predicate, ExpressionType comparison_type, int &min_val, int &max_val) {
    switch (comparison_type) {
        case ExpressionType::COMPARE_EQUAL: {
            
            break;
        }

        case ExpressionType::COMPARE_NOTEQUAL: {
            
            break;
        }

        case ExpressionType::COMPARE_LESSTHAN: {
            max_val = predicate - 1;
            break;
        }

        case ExpressionType::COMPARE_GREATERTHAN: {
            min_val = predicate + 1;
            break;
        }

        case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
            max_val = predicate;
            break;
        }

        case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
            min_val = predicate;
            break;
        }

        default: {
            std::cout << "Unknown comparison type for filter pushed down to table!" << std::endl;
            break;
        }
    }

    
}

ibis::bitvector ApplyFilter(const TableFilter &filter, rabit::Rabit *rabit_ptr, const duckdb::LogicalType &ctype) {
    int min_val = rabit_ptr->min_value_EE;
    int max_val = rabit_ptr->max_value_EE;

    switch (filter.filter_type) {
        case TableFilterType::CONJUNCTION_AND: {
            auto &conjunction_and = filter.Cast<ConjunctionAndFilter>();

            for (idx_t child_idx = 0; child_idx < conjunction_and.child_filters.size(); child_idx++) {
                auto &child_filter = *conjunction_and.child_filters[child_idx];
                if (child_filter.filter_type == TableFilterType::CONSTANT_COMPARISON) {
                    auto &constant_filter = child_filter.Cast<ConstantFilter>();
                    switch(ctype.InternalType()) {
                        case PhysicalType::INT32: {
                            auto predicate = IntegerValue::Get(constant_filter.constant);
                            FilterSelectionSwitch<int32_t>(predicate, constant_filter.comparison_type, min_val, max_val);
                            break;
                        }
                        case PhysicalType::INT64: {
                            auto predicate = BigIntValue::Get(constant_filter.constant);
                            if (ctype.id() == LogicalTypeId::DECIMAL) {
                                auto scale = DecimalType::GetScale(ctype);
                                int64_t scale_factor = static_cast<int64_t>(std::pow(10, scale));
                                predicate = predicate > scale_factor  ? predicate / scale_factor : predicate;
                            }
                            FilterSelectionSwitch<int64_t>(predicate, constant_filter.comparison_type, min_val, max_val);
                            break;
                        }
                    }
                } 
            }
            break;
        }
        case TableFilterType::CONSTANT_COMPARISON: {
            auto &constant_filter = filter.Cast<ConstantFilter>();
            
            switch(ctype.InternalType()) {
                case PhysicalType::INT32: {
                    auto predicate = IntegerValue::Get(constant_filter.constant);
                    FilterSelectionSwitch<int32_t>(predicate, constant_filter.comparison_type, min_val, max_val);
                    break;
                }
                case PhysicalType::INT64: {
                    auto predicate = BigIntValue::Get(constant_filter.constant);
                    if (ctype.id() == LogicalTypeId::DECIMAL) {
                        auto scale = DecimalType::GetScale(ctype);
                        int64_t scale_factor = static_cast<int64_t>(std::pow(10, scale));
                        predicate = predicate > scale_factor  ? predicate / scale_factor : predicate;
                    }
                    FilterSelectionSwitch<int64_t>(predicate, constant_filter.comparison_type, min_val, max_val);
                    break;
                }
            }
            break;
        }

    }

    ibis::bitvector btv_res;
    btv_res.copy(*rabit_ptr->Btvs[min_val]->btv);
    btv_res.decompress();
    for(int i = min_val + 1; i <= max_val; i++) {
        btv_res |= *rabit_ptr->Btvs[i]->btv;
    }
        
    return btv_res;
}

void BMTableScan::Table_Scan_GetRowids(ExecutionContext &context, vector<row_t> *row_ids, const TableScanBindData &bind_data, const PhysicalTableScan &op)
{
    if (!op.table_filters) {
        return;
    }

    vector<std::tuple<const TableFilter*, rabit::Rabit*, LogicalType>> rabit_list;

    for (auto &entry : op.table_filters->filters) {
        idx_t col_idx = entry.first;
        if (col_idx >= op.use_bitmap_columns.size() || !op.use_bitmap_columns[col_idx]) continue;
        if (col_idx >= op.column_ids.size()) continue;
        idx_t base_col = op.column_ids[col_idx].GetPrimaryIndex();
        string col_name = base_col < op.names.size() ? op.names[base_col] : to_string((long long)base_col);

        rabit::Rabit *ptr = nullptr;
        if (col_name == "l_shipdate") {
            ptr = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);
        } else if (col_name == "l_discount") {
            ptr = dynamic_cast<rabit::Rabit *>(context.client.bitmap_discount);
        } else if (col_name == "l_quantity") {
            ptr = dynamic_cast<rabit::Rabit *>(context.client.bitmap_quantity);
        }
        // TODO: add more bitmap columns here

        if (ptr) {
            LogicalType col_type = LogicalType::INVALID;
            col_type = bind_data.table.GetTypes()[base_col];
            rabit_list.emplace_back(entry.second.get(), ptr, col_type);
        }
    }
    bool first = true;
    ibis::bitvector final_btv;
    for (auto &p : rabit_list) {
        const TableFilter *filter = std::get<0>(p);
        rabit::Rabit *r_ptr = std::get<1>(p);
        const LogicalType &ctype = std::get<2>(p);
        ibis::bitvector btv_internal = ApplyFilter(*filter, r_ptr, ctype);

        if (first) {
            final_btv.copy(btv_internal);
            final_btv.decompress();
            first = false;
        }
        else {
            final_btv &= btv_internal;
        }
    }

    GetRowids(final_btv, row_ids);

}

SourceResultType BMTableScan::Table_Scan(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data, const PhysicalTableScan &op)
{
	if(*cursor == 0) {
			Table_Scan_GetRowids(context, row_ids, bind_data, op);
			num_idlist = row_ids->size();
		}
		
		if(*cursor < row_ids->size()) {

			vector<StorageIndex> storage_column_ids;
            for (auto &sel_id : op.projection_ids) {
                storage_column_ids.push_back(StorageIndex(op.column_ids[sel_id].GetPrimaryIndex()));
            }

			TableScanState local_storage_state;
			local_storage_state.Initialize(storage_column_ids);
			ColumnFetchState column_fetch_state;

			auto &table_bind_data = bind_data;
			auto &transaction = DuckTransaction::Get(context.client, table_bind_data.table.catalog);

			data_ptr_t row_ids_data = nullptr;
			row_ids_data = (data_ptr_t)&((*row_ids)[*cursor]);
			Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
			idx_t fetch_count = 2048;
			if(*cursor + fetch_count > row_ids->size()) {
				fetch_count = row_ids->size() - *cursor;
			}

			table_bind_data.table.GetStorage().BMFetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
                                                column_fetch_state, num_idlist);
			*cursor += fetch_count;
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		else {
			row_ids->clear();
            *cursor = 0;

            return SourceResultType::FINISHED;
		}
}


}