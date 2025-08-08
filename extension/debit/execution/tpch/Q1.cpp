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


struct q1_data {
int64_t sum_qty;
int64_t sum_base_price;
int64_t sum_disc_price;
int64_t sum_charge;
int64_t count_order;
int64_t sum_discount;
friend std::ostream& operator<<( std::ostream &output, const q1_data& D);
};
std::ostream& operator<<( std::ostream &output, const q1_data& D)
{
	output << std::fixed << std::setprecision(2) << (double)D.sum_qty / 100 << "  " << \
				 std::fixed << std::setprecision(2) << (double)D.sum_base_price / 100  << " " << \
				 std::fixed << std::setprecision(4) << (double)D.sum_disc_price / 10000 << "  " << \
				 std::fixed << std::setprecision(6) << (double)D.sum_charge / 1000000 << "  " <<\
				 (double)D.sum_qty / D.count_order / 100<< "  " << (double)D.sum_base_price / D.count_order / 100 << \
				 "  " << (double)D.sum_discount / D.count_order / 100 << "  " << D.count_order;

	return output;
}


inline void flip_bitvector(ibis::bitvector *btv)
{
#if defined(__AVX512F__)
	ibis::bitvector::word_t *it = btv->m_vec.begin();

	while(it + 15 < btv->m_vec.end()) {
		_mm512_storeu_epi32(it, _mm512_andnot_epi32(_mm512_loadu_epi32(it), \
													_mm512_set1_epi32(0x7fffffff)));
		it += 16;
	}

	for(; it < btv->m_vec.end(); it++)
	 *it ^= ibis::bitvector::ALLONES;

	if (btv->active.nbits > 0) { // also need to toggle active_word
		btv->active.val ^= ((1 << btv->active.nbits) - 1);
	}
#else
	ibis::bitvector::word_t *it = btv->m_vec.begin();
    
    while (it + 15 < btv->m_vec.end()) {
        for (int i = 0; i < 16; ++i) {
            it[i] = (~it[i]) & 0x7fffffff;
        }
        it += 16;
    }

    for (; it < btv->m_vec.end(); ++it) {
        *it ^= ibis::bitvector::ALLONES;
    }

    if (btv->active.nbits > 0) {
        btv->active.val ^= ((1 << btv->active.nbits) - 1);
    }
#endif
}

inline void reduce_zero(uint32_t *dst, uint32_t *src) {
#if defined(__AVX512F__)
	__m512i mask = _mm512_set_epi8(
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3,
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3,
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3,
		12, 13, 14, 15,8, 9, 10, 11,4, 5, 6, 7, 0, 1, 2, 3
	);

	_mm512_storeu_epi32(dst,\
	_mm512_shuffle_epi8( \
	_mm512_or_si512( \
		_mm512_sllv_epi32(_mm512_loadu_epi32(src), _mm512_set_epi32(16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1)),\
		_mm512_srlv_epi32(_mm512_loadu_epi32(src + 1), _mm512_set_epi32(15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30))), mask));

	_mm512_storeu_epi32(dst + 16,\
	_mm512_shuffle_epi8( \
	_mm512_or_si512( \
		_mm512_sllv_epi32(_mm512_loadu_epi32(src + 16), _mm512_set_epi32(0,31,30,29,28,27,26,25,24,23,22,21,20,19,18,17)),\
		_mm512_srlv_epi32(_mm512_loadu_epi32(src + 17), _mm512_set_epi32(0,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14))), mask));
#else
	const uint8_t shuffle_table[64] = {
        3,2,1,0,7,6,5,4,11,10,9,8,15,14,13,12,
        3,2,1,0,7,6,5,4,11,10,9,8,15,14,13,12,
        3,2,1,0,7,6,5,4,11,10,9,8,15,14,13,12,
        3,2,1,0,7,6,5,4,11,10,9,8,15,14,13,12
    };

    const int shift_left[32] = {
        1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,
        17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,0,
    };

    const int shift_right[32] = {
        30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,
        14,13,12,11,10,9,8,7,6,5,4,3,2,1,0,0
    };

    for (int i = 0; i < 32; i++) {
        uint32_t val1 = src[i] << shift_left[i];
        uint32_t val2 = src[i+1] >> shift_right[i];
        uint32_t combined = val1 | val2;

        uint32_t shuffled = 0;
        for (int j = 0; j < 4; j++) {
            uint8_t byte = (combined >> (j*8)) & 0xFF;
            uint8_t new_pos = shuffle_table[(i%16)*4 + j];
            shuffled |= (uint32_t(byte) << (new_pos*8));
        }
        dst[i] = shuffled;
    }
#endif
}

inline void q1_exe_aggregation(int64_t *quantity_ptr, int64_t *price_ptr, int64_t *discount_ptr, int64_t *tax_ptr, \
						uint16_t base, uint8_t bits, q1_data &sum) 
{
#if defined(__AVX512F__)
	if(!bits)
		return;

	__m512i indexes0 = _mm512_maskz_compress_epi64(bits, _mm512_loadu_epi64(quantity_ptr + base));
	sum.sum_qty += _mm512_reduce_add_epi64(indexes0);

	__m512i discount = _mm512_loadu_epi64(discount_ptr + base);
	__m512i index_discount = _mm512_maskz_compress_epi64(bits, discount);
	sum.sum_discount += _mm512_reduce_add_epi64(index_discount);
	
	indexes0 = _mm512_maskz_compress_epi64(bits, _mm512_loadu_epi64(price_ptr + base));
	sum.sum_base_price += _mm512_reduce_add_epi64(indexes0);

	__m512i indexes1 = _mm512_maskz_compress_epi64(bits, _mm512_sub_epi64(\
							_mm512_set_epi64(100, 100, 100, 100, 100, 100, 100, 100),\
								discount));
	indexes0 = _mm512_mullo_epi64(indexes0, indexes1);
	sum.sum_disc_price += _mm512_reduce_add_epi64(indexes0);

	indexes1 = _mm512_maskz_compress_epi64(bits, _mm512_add_epi64(\
					_mm512_set_epi64(100, 100, 100, 100, 100, 100, 100, 100),\
						_mm512_loadu_epi64(tax_ptr + base)));
	indexes0 = _mm512_mullo_epi64(indexes0, indexes1);
	sum.sum_charge += _mm512_reduce_add_epi64(indexes0);
#else
	if (!bits)
        return;

    for (int i = 0; i < 8; i++) {
        if (bits & (1 << i)) {
            int idx = base + i;
            
            sum.sum_qty += quantity_ptr[idx];
            
            int64_t discount = discount_ptr[idx];
            sum.sum_discount += discount;
            
            int64_t price = price_ptr[idx];
            sum.sum_base_price += price;
            
            int64_t disc_price = price * (100 - discount);
            sum.sum_disc_price += disc_price;
            
            int64_t tax = tax_ptr[idx];
            sum.sum_charge += disc_price * (100 + tax) / 100;
        }
    }		
#endif
}

unsigned char reverse_table[] = 
{
	0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0,
	0x08, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8,
	0x04, 0x84, 0x44, 0xC4, 0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4,
	0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC,
	0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2, 0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2,
	0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA,
	0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6,
	0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE, 0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE,
	0x01, 0x81, 0x41, 0xC1, 0x21, 0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
	0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9,
	0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5,
	0x0D, 0x8D, 0x4D, 0xCD, 0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD,
	0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3,
	0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB, 0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB,
	0x07, 0x87, 0x47, 0xC7, 0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7,
	0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF
};

bool q1_using_bitmap = 1;
bool q1_using_idlist = 0;
bool q1_using_segment = 1;
void BMTableScan::BMTPCH_Q1(ExecutionContext &context, const PhysicalTableScan &op)
{
	auto s0 = std::chrono::high_resolution_clock::now();

    auto &lineitem_table = Catalog::GetEntry<TableCatalogEntry>(context.client, "", "", "lineitem");
	
	// 1998-09-02
	int right_days = 10471;

	std::map<std::pair<char,char>, q1_data> q1_ans;

	if(q1_using_segment){
		long long time1 = 0;
		long long timeb = 0;
		long long timeids = 0;
		auto rabit_linestatus = dynamic_cast<rabit::Rabit *>(context.client.bitmap_linestatus);
		auto rabit_returnflag = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
		auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);

		auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
		TableScanState lineitem_scan_state;
		TableScanGlobalSourceState gs(context.client, op);
		vector<StorageIndex> storage_column_ids;
		storage_column_ids.push_back(StorageIndex(4));
		storage_column_ids.push_back(StorageIndex(5));
		storage_column_ids.push_back(StorageIndex(6));
		storage_column_ids.push_back(StorageIndex(7));
		storage_column_ids.push_back(StorageIndex(10));
		lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
		vector<LogicalType> types;
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);
		types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);

		std::map<std::pair<char, char>, std::pair<vector<row_t>, int64_t> > ids_map;
		std::map<std::pair<char, char>, std::pair<vector<uint32_t>*, uint8_t *> > rabit_res_map;

		std::map<int, char> returnflag_map;
		std::map<int, char> linestatus_map;
		returnflag_map[0] = 'N';
		returnflag_map[1] = 'R';
		returnflag_map[2] = 'A';
		linestatus_map[0] = 'O';
		linestatus_map[1] = 'F';

		auto stb = std::chrono::high_resolution_clock::now();
		
		SegBtv* shipdate_res = rabit_shipdate->range_res(0, 0, right_days + 1);

		for(int i = 0; i < rabit_linestatus->config->g_cardinality; i++) {
    		for(int r = 0; r < rabit_returnflag->config->g_cardinality; r++) {

				SegBtv *btv_shipdate = new SegBtv(*shipdate_res);
				btv_shipdate->deepCopy(*shipdate_res);
				btv_shipdate->decompress();

				SegBtv *btv_linestatus = new SegBtv(*rabit_linestatus->Btvs[i]->seg_btv);
				btv_linestatus->deepCopy(*rabit_linestatus->Btvs[i]->seg_btv);
				btv_linestatus->decompress();

				SegBtv *btv_returnflag = new SegBtv(*rabit_returnflag->Btvs[r]->seg_btv);
				btv_returnflag->deepCopy(*rabit_returnflag->Btvs[r]->seg_btv);
				btv_returnflag->decompress();
				
				auto &ttt_res = *btv_shipdate;
				ttt_res._and_in_thread(btv_linestatus, 0, ttt_res.seg_table.size());
				ttt_res._and_in_thread(btv_returnflag, 0, ttt_res.seg_table.size());

				uint64_t count = 0;
				for (const auto & [id_t, seg_t] : ttt_res.seg_table)
        			count += seg_t->btv->count();

				if(!count) continue;
			
				size_t total_bits = ttt_res.do_cnt();
                vector<uint32_t> *btv_res = new vector<uint32_t>(total_bits / 32 + (total_bits % 32 ? 1 : 0) + 4);

                uint32_t *dst_data = (uint32_t *)&(*btv_res)[0];
                size_t seg_idx = 0;
                size_t seg_cnt = ttt_res.seg_table.size();
				std::vector<uint32_t> remain_words;

                for (const auto& seg_pair : ttt_res.seg_table) {
                    ibis::bitvector* seg_btv = seg_pair.second->btv;
					uint32_t *src_data = seg_btv->m_vec.begin();
					uint32_t* src_end = seg_btv->m_vec.end();

					while(remain_words.size() < 32 && src_data < src_end) {
						remain_words.push_back(*src_data);
						src_data++;
					}

					if(remain_words.size() == 32) {
						reduce_zero(dst_data, remain_words.data());
						dst_data += 31;
						remain_words.clear();
					}

					while(src_data + 31 < src_end) {
						reduce_zero(dst_data, src_data);
						dst_data += 31;
						src_data += 32;
					}

					while(src_data < src_end) {
						remain_words.push_back(*src_data);
						src_data++;
					}
				}

				int need_bits = 31;
				uint32_t *src_data2 = remain_words.data();
				uint32_t *src_end2 = remain_words.data() + remain_words.size();
				while(src_data2 + 1 < src_end2) {
					dst_data[0] = ((src_data2[0] << (32 - need_bits)) | (src_data2[1] >> (need_bits - 1)));
					dst_data[0] = htonl(dst_data[0]);
					need_bits--;
					src_data2++;
					dst_data++;
				}

				auto &last_seg = ttt_res.seg_table.rbegin()->second->btv;
				dst_data[0] = src_data2[0] << (32 - need_bits);
				uint32_t last_word = last_seg->active.val << (32 - last_seg->active.nbits);
				if(last_seg->active.nbits + need_bits > 32) {
					dst_data[0] |= (last_word >> need_bits);
					dst_data[0] = htonl(dst_data[0]);
					last_word <<= (32 - need_bits);
					dst_data[1] = last_word;
					dst_data[1] = htonl(dst_data[1]);
				} else {
					dst_data[0] |= (last_word >> need_bits);
					dst_data[0] = htonl(dst_data[0]);
				}


				rabit_res_map[{returnflag_map[r], linestatus_map[i]}].first = btv_res;
				rabit_res_map[{returnflag_map[r], linestatus_map[i]}].second = (uint8_t *)&((*btv_res)[0]);
				q1_ans[{returnflag_map[r], linestatus_map[i]}].count_order = count;
				
			}
		}
		auto etb = std::chrono::high_resolution_clock::now();
		timeb += std::chrono::duration_cast<std::chrono::nanoseconds>(etb - stb).count();

		int64_t cursor = 0;
		int64_t offset = 0;
		while(true) {
			DataChunk result;
			result.Initialize(context.client, types);
			lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
			if(result.size() == 0)
				break;
			
			offset = cursor;
			cursor += result.size();



			auto &quantity = result.data[0];
			auto &extendedprice = result.data[1];
			auto &discount = result.data[2];
			auto &tax = result.data[3];
			auto &days = result.data[4];

			auto quantity_data = FlatVector::GetData<int64_t>(quantity);
			auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
			auto discount_data = FlatVector::GetData<int64_t>(discount);
			auto tax_data = FlatVector::GetData<int64_t>(tax);
			auto days_data = FlatVector::GetData<int32_t>(days);


			auto st1 = std::chrono::high_resolution_clock::now();

			for(auto &ids_it : rabit_res_map) {
				auto &btv_it = ids_it.second.second;
				auto &ans_it = q1_ans[ids_it.first];
				uint16_t base = 0;
				while(base + 7 < result.size() ) {
					q1_exe_aggregation(quantity_data, extendedprice_data, discount_data, tax_data,\
										base, reverse_table[*btv_it], q1_ans[ids_it.first]);
					btv_it++;
					base += 8;
				}
				if(base < result.size()) {
					uint8_t bits = *btv_it;
					while(base < result.size()) {
						if( bits & 0x80 ) {
							ans_it.sum_qty += quantity_data[base];
							ans_it.sum_discount += discount_data[base];
							ans_it.sum_base_price += extendedprice_data[base];
							ans_it.sum_disc_price += extendedprice_data[base]*(100 - discount_data[base]);
							ans_it.sum_charge += extendedprice_data[base]*(100 - discount_data[base]) *(100 + tax_data[base]);
						}
						base++;
						bits <<= 1;
					}
				}
			}
			
			auto et1 = std::chrono::high_resolution_clock::now();
			time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
		
		}
		std::cout << "bitmap time : "<< timeb/1000000 << "ms" << std::endl;
		std::cout << "ids time : "<< timeids/1000000 << "ms" << std::endl;
		std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;

		 auto s1 = std::chrono::high_resolution_clock::now();

		for(auto &it : q1_ans) {
			std::cout << it.first.first << it.first.second << " : " << it.second << std::endl;
		}

		std::cout << "q1 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;

		return;

	}

	if(!q1_using_bitmap) {
        long long time1 = 0;
        auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
        TableScanState lineitem_scan_state;
        TableScanGlobalSourceState gs(context.client, op);
        vector<StorageIndex> storage_column_ids;
        storage_column_ids.push_back(StorageIndex(8));
        storage_column_ids.push_back(StorageIndex(9));
        storage_column_ids.push_back(StorageIndex(4));
        storage_column_ids.push_back(StorageIndex(5));
        storage_column_ids.push_back(StorageIndex(6));
        storage_column_ids.push_back(StorageIndex(7));
        storage_column_ids.push_back(StorageIndex(10));
        lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
        vector<LogicalType> types;
        types.push_back(lineitem_table.GetColumns().GetColumnTypes()[8]);
        types.push_back(lineitem_table.GetColumns().GetColumnTypes()[9]);
        types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
        types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
        types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
        types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);
        types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);

        while(true) {
            DataChunk result;
            result.Initialize(context.client, types);
            lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
            if(result.size() == 0)
                break;

            auto &returnflag = result.data[0];
            auto &linestatus = result.data[1];
            auto &quantity = result.data[2];
            auto &extendedprice = result.data[3];
            auto &discount = result.data[4];
            auto &tax = result.data[5];
            auto &days = result.data[6];

            auto quantity_data = FlatVector::GetData<int64_t>(quantity);
            auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
            auto discount_data = FlatVector::GetData<int64_t>(discount);
            auto tax_data = FlatVector::GetData<int64_t>(tax);
            auto days_data = FlatVector::GetData<int32_t>(days);

            if(result.data[0].GetVectorType() == VectorType::DICTIONARY_VECTOR) {
                auto &returnflag_sel_vector = DictionaryVector::SelVector(result.data[0]);
                auto &returnflag_child = DictionaryVector::Child(result.data[0]);
                auto &linestatus_sel_vector = DictionaryVector::SelVector(result.data[1]);
                auto &linestatus_child = DictionaryVector::Child(result.data[1]);
                
                auto st1 = std::chrono::high_resolution_clock::now();
                
                for(int i = 0; i < result.size(); i++) {

                    if(days_data[i] > right_days)
                        continue;

                    auto &it = q1_ans[{reinterpret_cast<string_t *>(returnflag_child.GetData())[returnflag_sel_vector.get_index(i)].GetData()[0],\
                            reinterpret_cast<string_t *>(linestatus_child.GetData())[linestatus_sel_vector.get_index(i)].GetData()[0]}];

                    it.sum_qty += quantity_data[i];
                    it.sum_discount += discount_data[i];
                    it.sum_base_price += extendedprice_data[i];
                    it.sum_disc_price += extendedprice_data[i]*(100 - discount_data[i]);
                    it.sum_charge += extendedprice_data[i]*(100 - discount_data[i]) *(100 + tax_data[i]);
                    it.count_order += 1;
                }
                
                auto et1 = std::chrono::high_resolution_clock::now();
                time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
            }
            else {
                auto st1 = std::chrono::high_resolution_clock::now();
                
                for(int i = 0; i < result.size(); i++) {

                    if(days_data[i] > right_days)
                        continue;

                    auto &it = q1_ans[{reinterpret_cast<string_t *>(returnflag.GetData())[i].GetData()[0], \
                        reinterpret_cast<string_t *>(linestatus.GetData())[i].GetData()[0]}];

                    it.sum_qty += quantity_data[i];
                    it.sum_discount += discount_data[i];
                    it.sum_base_price += extendedprice_data[i];
                    it.sum_disc_price += extendedprice_data[i]*(100 - discount_data[i]);
                    it.sum_charge += extendedprice_data[i]*(100 - discount_data[i]) *(100 + tax_data[i]);
                    it.count_order += 1;
                }
                
                auto et1 = std::chrono::high_resolution_clock::now();
                time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
            }
        }
        std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;
	}
    else {
        if(q1_using_idlist) {
			long long time1 = 0;
			long long timeb = 0;
			long long timeids = 0;
			auto rabit_linestatus = dynamic_cast<rabit::Rabit *>(context.client.bitmap_linestatus);
			auto rabit_returnflag = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
			auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
			TableScanState lineitem_scan_state;
			TableScanGlobalSourceState gs(context.client, op);
			vector<StorageIndex> storage_column_ids;
			storage_column_ids.push_back(StorageIndex(4));
			storage_column_ids.push_back(StorageIndex(5));
			storage_column_ids.push_back(StorageIndex(6));
			storage_column_ids.push_back(StorageIndex(7));
			storage_column_ids.push_back(StorageIndex(10));
			lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
			vector<LogicalType> types;
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);

			std::map<std::pair<char, char>, std::pair<vector<row_t>, int64_t> > ids_map;
			std::map<int, char> returnflag_map;
			std::map<int, char> linestatus_map;
			returnflag_map[0] = 'N';
			returnflag_map[1] = 'R';
			returnflag_map[2] = 'A';
			linestatus_map[0] = 'O';
			linestatus_map[1] = 'F';

			auto stb = std::chrono::high_resolution_clock::now();
			for(int i = 0; i < rabit_linestatus->config->g_cardinality; i++) {
				for(int r = 0; r < rabit_returnflag->config->g_cardinality; r++) {
					ibis::bitvector btv_res;
					btv_res.copy(*rabit_returnflag->Btvs[r]->btv);
					btv_res &= *rabit_linestatus->Btvs[i]->btv;

					if(!btv_res.count())
						continue;

					auto &ids = ids_map[{returnflag_map[r], linestatus_map[i]}].first;

					auto stids = std::chrono::high_resolution_clock::now();

					ids.resize(btv_res.count() + 64);

					auto element_ptr = &ids[0];

					uint32_t ids_count = 0;
					uint64_t ids_idx = 0;
					// traverse m_vec
					auto it = btv_res.m_vec.begin();
					while(it != btv_res.m_vec.end()) {
						util_btv_to_id_list(element_ptr, ids_count, ids_idx, reverseBits(*it));
						ids_idx += 31;
						it++;
					}

					// active word
					util_btv_to_id_list(element_ptr, ids_count, ids_idx, \
											reverseBits(btv_res.active.val << (31 - btv_res.active.nbits)));

					ids.resize(btv_res.count());


					auto etids = std::chrono::high_resolution_clock::now();


					timeids += std::chrono::duration_cast<std::chrono::nanoseconds>(etids - stids).count();

				}
			}
			auto etb = std::chrono::high_resolution_clock::now();
			timeb += std::chrono::duration_cast<std::chrono::nanoseconds>(etb - stb).count();


			int64_t cursor = 0;
			int64_t offset = 0;
			while(true) {
				DataChunk result;
				result.Initialize(context.client, types);
				// lineitem_scan_state.table_state.Scan(lineitem_transaction, result);
				lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
				if(result.size() == 0)
					break;
				
				offset = cursor;
				cursor += result.size();

				auto &quantity = result.data[0];
				auto &extendedprice = result.data[1];
				auto &discount = result.data[2];
				auto &tax = result.data[3];
				auto &days = result.data[4];

				auto quantity_data = FlatVector::GetData<int64_t>(quantity);
				auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
				auto discount_data = FlatVector::GetData<int64_t>(discount);
				auto tax_data = FlatVector::GetData<int64_t>(tax);
				auto days_data = FlatVector::GetData<int32_t>(days);


				auto st1 = std::chrono::high_resolution_clock::now();

				for(auto &ids_it : ids_map) {
					auto &it = q1_ans[ids_it.first];
					auto &ids = ids_it.second.first;
					auto &c_row = ids_it.second.second;
					while(c_row < ids.size() && ids[c_row] < cursor) {
						uint64_t i = ids[c_row++] - offset;

						if(days_data[i] > right_days)
							continue;

						it.sum_qty += quantity_data[i];
						it.sum_discount += discount_data[i];
						it.sum_base_price += extendedprice_data[i];
						it.sum_disc_price += extendedprice_data[i]*(100 - discount_data[i]);
						it.sum_charge += extendedprice_data[i]*(100 - discount_data[i]) *(100 + tax_data[i]);
						it.count_order += 1;
					}
				}
				
				auto et1 = std::chrono::high_resolution_clock::now();
				time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
			
			}
			std::cout << "bitmap time : "<< timeb/1000000 << "ms" << std::endl;
			std::cout << "ids time : "<< timeids/1000000 << "ms" << std::endl;
			std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;
		}
        else
		{
			long long time1 = 0;
			long long timeb = 0;
			long long timeids = 0;
			auto rabit_linestatus = dynamic_cast<rabit::Rabit *>(context.client.bitmap_linestatus);
			auto rabit_returnflag = dynamic_cast<rabit::Rabit *>(context.client.bitmap_returnflag);
			auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);

			auto &lineitem_transaction = DuckTransaction::Get(context.client, lineitem_table.catalog);
			TableScanState lineitem_scan_state;
			TableScanGlobalSourceState gs(context.client, op);
			vector<StorageIndex> storage_column_ids;
			storage_column_ids.push_back(StorageIndex(4));
			storage_column_ids.push_back(StorageIndex(5));
			storage_column_ids.push_back(StorageIndex(6));
			storage_column_ids.push_back(StorageIndex(7));
			storage_column_ids.push_back(StorageIndex(10));
			lineitem_table.GetStorage().InitializeScan(context.client, lineitem_transaction, lineitem_scan_state, storage_column_ids);
			vector<LogicalType> types;
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[4]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[5]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[6]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[7]);
			types.push_back(lineitem_table.GetColumns().GetColumnTypes()[10]);

			std::map<std::pair<char, char>, std::pair<vector<row_t>, int64_t> > ids_map;
			std::map<std::pair<char, char>, std::pair<vector<uint32_t>*, uint8_t *> > rabit_res_map;
			std::map<int, char> returnflag_map;
			std::map<int, char> linestatus_map;
			returnflag_map[0] = 'N';
			returnflag_map[1] = 'R';
			returnflag_map[2] = 'A';
			linestatus_map[0] = 'O';
			linestatus_map[1] = 'F';

			auto stb = std::chrono::high_resolution_clock::now();

			ibis::bitvector shipdate_res;
			shipdate_res.copy(*rabit_shipdate->Btvs[rabit_shipdate->config->g_cardinality - 1]->btv);
			shipdate_res.decompress();
			for(int i = rabit_shipdate->config->g_cardinality - 2; i > right_days; i--)
				shipdate_res |= *rabit_shipdate->Btvs[i]->btv;

			flip_bitvector(&shipdate_res);

			for(int i = 0; i < rabit_linestatus->config->g_cardinality; i++) {
				for(int r = 0; r < rabit_returnflag->config->g_cardinality; r++) {

					ibis::bitvector ttt_res;
					ttt_res.copy(*rabit_linestatus->Btvs[i]->btv);
					ttt_res &= *rabit_returnflag->Btvs[r]->btv;
					ttt_res &= shipdate_res;

					uint64_t count = ttt_res.count();

					if(!count) {
						continue;
					}

					vector<uint32_t> *btv_res = new vector<uint32_t>(ttt_res.size() / 32 + (ttt_res.size() % 32?1:0) + 4);

					uint32_t *dst_data = (uint32_t *)&(*btv_res)[0];
					uint32_t *src_data = ttt_res.m_vec.begin();

					// load 31 dst once
					while(src_data + 31 < ttt_res.m_vec.end()) {
						reduce_zero(dst_data, src_data);
						dst_data += 31;
						src_data += 32;
					}

					// 1 dst once
					int need_bits = 31;
					while(src_data + 1 < ttt_res.m_vec.end()) {
						dst_data[0] = ((src_data[0] << (32 - need_bits)) | (src_data[1] >> (need_bits - 1)));
						dst_data[0] = htonl(dst_data[0]);
						need_bits--;
						src_data++;
						dst_data++;
					}

					// active word
					dst_data[0] = src_data[0] << (32 - need_bits);
					uint32_t last_word = ttt_res.active.val << (32 - ttt_res.active.nbits);
					if(ttt_res.active.nbits + need_bits > 32) {
						dst_data[0] |= (last_word >> need_bits);
						dst_data[0] = htonl(dst_data[0]);
						last_word <<= (32 - need_bits);
						dst_data[1] = last_word;
						dst_data[1] = htonl(dst_data[1]);
					}
					else {
						dst_data[0] |= (last_word >> need_bits);
						dst_data[0] = htonl(dst_data[0]);
					}
					

					rabit_res_map[{returnflag_map[r], linestatus_map[i]}].first = btv_res;
					rabit_res_map[{returnflag_map[r], linestatus_map[i]}].second = (uint8_t *)&((*btv_res)[0]);
					q1_ans[{returnflag_map[r], linestatus_map[i]}].count_order = count;

				}
			}
			auto etb = std::chrono::high_resolution_clock::now();
			timeb += std::chrono::duration_cast<std::chrono::nanoseconds>(etb - stb).count();


			int64_t cursor = 0;
			int64_t offset = 0;
			while(true) {
				DataChunk result;
				result.Initialize(context.client, types);
				lineitem_table.GetStorage().Scan(lineitem_transaction, result, lineitem_scan_state);
				if(result.size() == 0)
					break;
				
				offset = cursor;
				cursor += result.size();



				auto &quantity = result.data[0];
				auto &extendedprice = result.data[1];
				auto &discount = result.data[2];
				auto &tax = result.data[3];
				auto &days = result.data[4];

				auto quantity_data = FlatVector::GetData<int64_t>(quantity);
				auto extendedprice_data = FlatVector::GetData<int64_t>(extendedprice);
				auto discount_data = FlatVector::GetData<int64_t>(discount);
				auto tax_data = FlatVector::GetData<int64_t>(tax);
				auto days_data = FlatVector::GetData<int32_t>(days);


				auto st1 = std::chrono::high_resolution_clock::now();

				for(auto &ids_it : rabit_res_map) {
					auto &btv_it = ids_it.second.second;
					auto &ans_it = q1_ans[ids_it.first];
					uint16_t base = 0;
					while(base + 7 < result.size() ) {
						q1_exe_aggregation(quantity_data, extendedprice_data, discount_data, tax_data,\
											base, reverse_table[*btv_it], q1_ans[ids_it.first]);
						btv_it++;
						base += 8;
					}
					if(base < result.size()) {
						uint8_t bits = *btv_it;
						while(base < result.size()) {
							if( bits & 0x80 ) {
								ans_it.sum_qty += quantity_data[base];
								ans_it.sum_discount += discount_data[base];
								ans_it.sum_base_price += extendedprice_data[base];
								ans_it.sum_disc_price += extendedprice_data[base]*(100 - discount_data[base]);
								ans_it.sum_charge += extendedprice_data[base]*(100 - discount_data[base]) *(100 + tax_data[base]);
							}
							base++;
							bits <<= 1;
						}
					}
				}
				
				auto et1 = std::chrono::high_resolution_clock::now();
				time1 += std::chrono::duration_cast<std::chrono::nanoseconds>(et1 - st1).count();
			
			}
			std::cout << "bitmap time : "<< timeb/1000000 << "ms" << std::endl;
			std::cout << "ids time : "<< timeids/1000000 << "ms" << std::endl;
			std::cout << "compute time : "<< time1/1000000 << "ms" << std::endl;
		}
    }
    
    auto s1 = std::chrono::high_resolution_clock::now();

	for(auto &it : q1_ans) {
		std::cout << it.first.first << it.first.second << " : " << it.second << std::endl;
	}

	std::cout << "q1 time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;

}

void BMTableScan::TPCH_Q1_Lineitem_GetRowIds(ExecutionContext &context, vector<row_t> *row_ids)
{
	auto rabit_shipdate = dynamic_cast<rabit::Rabit *>(context.client.bitmap_shipdate);

	int right_days = 10471;

	auto s0 = std::chrono::high_resolution_clock::now();

	SegBtv *btv_shipdate = rabit_shipdate->range_res(0, 0, right_days + 1);
	 
	auto s1 = std::chrono::high_resolution_clock::now();

	auto &btv_res = *btv_shipdate;

	GetRowidsSeg(btv_res, row_ids);

	auto s2 = std::chrono::high_resolution_clock::now();

	if(btv_shipdate){
        delete btv_shipdate;
        btv_shipdate = nullptr;
    }

	std::cout << "shipdate's bitmap time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s1 - s0).count() << "ms" << std::endl;
	std::cout << "bitmaps time : " << std::chrono::duration_cast<std::chrono::milliseconds>(s2 - s0).count() << "ms" << std::endl;
	std::cout << "get row id : " << std::chrono::duration_cast<std::chrono::milliseconds>(s2 - s1).count() << "ms" << std::endl;

}

SourceResultType BMTableScan::BMTPCH_Q1(ExecutionContext &context, DataChunk &chunk, const TableScanBindData &bind_data)
{
	if(*cursor == 0) {
			TPCH_Q1_Lineitem_GetRowIds(context, row_ids);
		}
		
		if(*cursor < row_ids->size()) {

			vector<StorageIndex> storage_column_ids;

			storage_column_ids.push_back(StorageIndex(8));	// For l_returnflag
			storage_column_ids.push_back(StorageIndex(9));	// For l_linestatus
			storage_column_ids.push_back(StorageIndex(4));	// For l_quantity
			storage_column_ids.push_back(StorageIndex(5));	// For l_extendedprice
			storage_column_ids.push_back(StorageIndex(6));	// For l_discount
			storage_column_ids.push_back(StorageIndex(7));	// For l_tax

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

			table_bind_data.table.GetStorage().Fetch(transaction, chunk, storage_column_ids, row_ids_vec, fetch_count,
													column_fetch_state);
			*cursor += fetch_count;
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
		else {
			row_ids->clear();
            *cursor = 0;
            context.client.query_source = "tpch";

            return SourceResultType::FINISHED;
		}
}



}
