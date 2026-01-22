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
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <iostream>
#include <iomanip>
#include <chrono>
#include <netinet/in.h>

namespace duckdb {

class TableScanGlobalSourceState : public GlobalSourceState {
public:
	TableScanGlobalSourceState(ClientContext &context, const PhysicalTableScan &op) {
		if (op.dynamic_filters && op.dynamic_filters->HasFilters()) {
			table_filters = op.dynamic_filters->GetFinalTableFilters(op, op.table_filters.get());
		}

		if (op.function.init_global) {
			auto filters = table_filters ? *table_filters : GetTableFilters(op);
			TableFunctionInitInput input(op.bind_data.get(), op.column_ids, op.projection_ids, filters,
			                             op.extra_info.sample_options);

			global_state = op.function.init_global(context, input);
			if (global_state) {
				max_threads = global_state->MaxThreads();
			}
		} else {
			max_threads = 1;
		}
		if (op.function.in_out_function) {
			// this is an in-out function, we need to setup the input chunk
			vector<LogicalType> input_types;
			for (auto &param : op.parameters) {
				input_types.push_back(param.type());
			}
			input_chunk.Initialize(context, input_types);
			for (idx_t c = 0; c < op.parameters.size(); c++) {
				input_chunk.data[c].Reference(op.parameters[c]);
			}
			input_chunk.SetCardinality(1);
		}
	}

	idx_t max_threads = 0;
	unique_ptr<GlobalTableFunctionState> global_state;
	bool in_out_final = false;
	DataChunk input_chunk;
	//! Combined table filters, if we have dynamic filters
	unique_ptr<TableFilterSet> table_filters;

	optional_ptr<TableFilterSet> GetTableFilters(const PhysicalTableScan &op) const {
		return table_filters ? table_filters.get() : op.table_filters.get();
	}
	idx_t MaxThreads() override {
		return max_threads;
	}
};


struct projection_data {
int64_t sum_price_discount = 0;
int64_t count_order;

friend std::ostream& operator<<( std::ostream &output, const projection_data& D);
};
std::ostream& operator<<( std::ostream &output, const projection_data& D)
{
	output << std::fixed << std::setprecision(4) << (double)D.sum_price_discount / 10000;

	return output;
}


void BMTableScan::Projection_test(ExecutionContext &context, const PhysicalTableScan &op)
{
    double time_fetch = 0;

    auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	
	auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);
	auto rabit_quantity = dynamic_cast<rabit::Rabit *>(context.client.bitmap_quantity);

	auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
	TableScanState lineitem_scan_state;
	TableScanGlobalSourceState gs(context.client, op);
	vector<StorageIndex> storage_column_ids;
	storage_column_ids.push_back(StorageIndex(5));
	storage_column_ids.push_back(StorageIndex(6));
	lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
	vector<LogicalType> types;
	types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
	types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);

	int start_day = 8766; // 1994-01-01
    int end_day = 9489; // 1995-12-25

	int upper_quantity = 4;

	ibis::bitvector btv_shipdate;
	btv_shipdate.copy(*rabit_shipdate->Btvs[start_day]->btv);
	btv_shipdate.decompress();

    for(uint32_t i = start_day + 1; i < end_day; i++) {
		btv_shipdate |= *rabit_shipdate->Btvs[i]->btv;
	}

    ibis::bitvector btv_quantity;
    btv_quantity.copy(*rabit_quantity->Btvs[upper_quantity - 1]->btv);

    for(uint32_t i = upper_quantity - 2; i >= 1; i--) {
		btv_quantity |= *rabit_quantity->Btvs[i]->btv;
	}

    btv_shipdate &= btv_quantity;

    vector<row_t> *ids = new vector<row_t>;

    GetRowids(btv_shipdate, ids);

    projection_data agg_ans;
    size_t cursor = 0;
    while(true) {
        auto st_fetch = std::chrono::high_resolution_clock::now();
        
        DataChunk result;
        result.Initialize(context.client, types);

        if(cursor < ids->size()) {
            ColumnFetchState column_fetch_state;
            data_ptr_t row_ids_data = nullptr;
            row_ids_data = (data_ptr_t)&((*ids)[cursor]);
            Vector row_ids_vec(LogicalType::ROW_TYPE, row_ids_data);
            idx_t fetch_count = 2048;
            if(cursor + fetch_count > ids->size()) {
                fetch_count = ids->size() - cursor;
            }

            lineitem_table.GetStorage().Fetch(lineitem_transaction, result, storage_column_ids, row_ids_vec, fetch_count,
                                                    column_fetch_state);

            cursor += fetch_count;
        }
        else {
            delete ids;
            break;
        }

        auto &l_extendedprice = result.data[0];
        auto &l_discount = result.data[1];
        auto l_extendedprice_data = FlatVector::GetData<int64_t>(l_extendedprice);
        auto l_discount_data = FlatVector::GetData<int64_t>(l_discount);

        for(int i = 0; i < result.size(); i++) {
            agg_ans.sum_price_discount += l_extendedprice_data[i] * l_discount_data[i];
        }

        auto et_fetch = std::chrono::high_resolution_clock::now();
        time_fetch += std::chrono::duration_cast<std::chrono::nanoseconds>(et_fetch - st_fetch).count();
    }

    std::cout <<"revenue:"<< agg_ans << std::endl;
    std::cout << "time:" << time_fetch / 1000000 << "ms" << std::endl;

	return;
	
}

}
