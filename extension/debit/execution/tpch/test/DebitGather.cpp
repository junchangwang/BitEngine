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

#include <iostream>
#include <chrono>

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

void gather_i64_from_i64_idx(const int64_t* A, const uint64_t* B, int n, int64_t* out, int64_t cursor) {
    int i = 0;
    __m512i vcursor = _mm512_set1_epi64(cursor);
    for (; i + 8 <= n; i += 8) {
        __m512i idx = _mm512_loadu_si512((const void*)(B + i)); 
        idx = _mm512_sub_epi64(idx, vcursor); 
        __m512i gathered = _mm512_i64gather_epi64(idx, (const void*)A, 8);
        _mm512_storeu_si512((void*)(out + i), gathered);
    }
    int rem = n - i;
    if (rem) {
        for (int j = 0; j < rem; ++j) {
            out[i + j] = A[B[i + j] - cursor];
        }
    }
}

void gather_i64_from_i32(const int64_t* A, const uint32_t* B, int n, int64_t* out, int64_t cursor) {
    int i = 0;
    __m512i vcursor = _mm512_set1_epi64(cursor);
    for (; i + 8 <= n; i += 8) {
        __m256i idx32 = _mm256_loadu_si256((const __m256i*)(B + i)); // 8 x u32
        __m512i idx = _mm512_cvtepu32_epi64(idx32); // 8 x u64 -> 8 x i64
        idx = _mm512_sub_epi64(idx, vcursor);
        __m512i gathered = _mm512_i64gather_epi64(idx, (const void*)A, 8);
        _mm512_storeu_si512((void*)(out + i), gathered);
    }
    int rem = n - i;
    if (rem) {
        for (int j = 0; j < rem; ++j) {
            out[i + j] = A[B[i + j] - cursor];
        }
    }
}

void gather_i32_from_i32(const int32_t* A, const uint32_t* B, int n, int32_t* out, int64_t cursor) {
   int i = 0;
    __m512i vcursor = _mm512_set1_epi32(static_cast<int32_t>(cursor));
    for (; i + 16 <= n; i += 16) {
        __m512i idx = _mm512_loadu_si512((const void*)(B + i)); 
        idx = _mm512_sub_epi32(idx, vcursor); 
        __m512i gathered = _mm512_i32gather_epi32(idx, (const void*)A, 4);
        _mm512_storeu_si512((void*)(out + i), gathered);
    }
    int rem = n - i;
    if (rem) {
        for (int j = 0; j < rem; ++j) {
            out[i + j] = A[B[i + j] - cursor];
        }
    }
}

void aggregation(int32_t *price_ptr, int32_t *discount_ptr, uint16_t base, int64_t &sum) 
{
#if defined(__AVX512F__)

	__m512i price = _mm512_loadu_epi32(price_ptr + base);
	__m512i discount = _mm512_loadu_epi32(discount_ptr + base);

	__m512i price_times_discount = _mm512_mullo_epi32(price, discount);
	sum += _mm512_reduce_add_epi32(price_times_discount);
#else
    int64_t total = 0;
    for (int i = 0; i < 16; ++i) {
        total += int32_t(price_ptr[base + i]) * int32_t(discount_ptr[base + i]);
    }
    sum += total;		
#endif
}

void BMTableScan::BMGather(ExecutionContext &context, const PhysicalTableScan &op, std::vector<uint32_t>* sizes, std::vector<uint32_t>* g_idlist)
{   
    int i = -1;
    if(i == -1) {
        // CheckTime(context, row_ids);
        i++;
    }

    auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	
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

    int64_t offset = 0;
    int64_t cursor = 0;
    double time_count1 = 0;
    double time_count2 = 0;

    while (i < sizes->size()) {
        auto t0 = std::chrono::high_resolution_clock::now();

        DataChunk result;
        result.Initialize(context.client, types);
        lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);

        auto &extendedprice = result.data[0];
        auto &discount = result.data[1];

        auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
        auto discount_data = FlatVector::GetData<int64_t>(discount);

        auto t1 = std::chrono::high_resolution_clock::now();

        int32_t* extendedprice_data_32 = new int32_t[result.size()];
        int32_t* discount_data_32 = new int32_t[result.size()];
        for (int i = 0; i < result.size(); ++i) {
            extendedprice_data_32[i] = static_cast<int32_t>(extendedprice_data[i]);
            discount_data_32[i] = static_cast<int32_t>(discount_data[i]);
        }

        auto t2 = std::chrono::high_resolution_clock::now();

        int approved_tuple_count = sizes->at(i);
        const uint32_t* row_ids_ptr = reinterpret_cast<const uint32_t*>(g_idlist->data() + offset);

        int32_t* gathered_extendedprice = (int32_t*)aligned_alloc(4, approved_tuple_count * sizeof(int32_t));
	    int32_t* gathered_discount = (int32_t*)aligned_alloc(4, approved_tuple_count * sizeof(int32_t));

        gather_i32_from_i32(extendedprice_data_32, row_ids_ptr, approved_tuple_count, gathered_extendedprice, cursor);
        gather_i32_from_i32(discount_data_32, row_ids_ptr, approved_tuple_count, gathered_discount, cursor);

        uint16_t base = 0;
        while(base + 15 < approved_tuple_count) {
            aggregation(gathered_extendedprice, gathered_discount, base, sum_q6);
            base += 16;
        }
        while(base < approved_tuple_count) {
            sum_q6 += gathered_extendedprice[base] * gathered_discount[base];
            base++;
        }

        auto t3 = std::chrono::high_resolution_clock::now();
        time_count1 += std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
        time_count2 += std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).count();
        std::free(gathered_extendedprice);
        std::free(gathered_discount);
        delete[] extendedprice_data_32;
        delete[] discount_data_32;

        cursor += result.size();
        offset += approved_tuple_count;
        i++;
    }

    // int64_t cursor = 0;
    // int64_t offset = 0;
    // const uint64_t* row_ids_last = reinterpret_cast<const uint64_t*>(row_ids->data() + row_ids->size());
    // while (true) {
    //     DataChunk result;
    //     result.Initialize(context.client, types);
    //     lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);

    //     if(result.size() == 0) break;

    //     auto &extendedprice = result.data[0];
    //     auto &discount = result.data[1];

    //     auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
    //     auto discount_data = FlatVector::GetData<int64_t>(discount);

    //     const uint64_t* row_ids_ptr = reinterpret_cast<const uint64_t*>(row_ids->data() + offset);
        
    //     auto it = std::lower_bound(row_ids_ptr,  row_ids_last, cursor + result.size());
    //     int approved_tuple_count = it - row_ids_ptr;

    //     int64_t* gathered_extendedprice = (int64_t*)aligned_alloc(8, approved_tuple_count * sizeof(int64_t));
	//     int64_t* gathered_discount = (int64_t*)aligned_alloc(8, approved_tuple_count * sizeof(int64_t));

    //     gather_i64_from_i64_idx(extendedprice_data, row_ids_ptr, approved_tuple_count, gathered_extendedprice, cursor);
    //     gather_i64_from_i64_idx(discount_data, row_ids_ptr, approved_tuple_count, gathered_discount, cursor);

    //     uint16_t base = 0;
    //     while(base + 7 < approved_tuple_count) {
    //         exe_aggregation(gathered_extendedprice, gathered_discount, base, sum_q6);
    //         base += 8;
    //     }
    //     while(base < approved_tuple_count) {
    //         sum_q6 += gathered_extendedprice[base] * gathered_discount[base];
    //         base++;
    //     }

    //     std::free(gathered_extendedprice);
    //     std::free(gathered_discount);

    //     cursor += result.size();
    //     offset += approved_tuple_count;
    // }

    std::cout << "time1 : "<< time_count1/1000000 << "ms" << std::endl; 
    std::cout << "time2 : "<< time_count2/1000000 << "ms" << std::endl;
}  

}
