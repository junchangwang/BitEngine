#include "execution/tpch/bitmap_hash_join.hpp"

namespace duckdb {

BMHashJoin::BMHashJoin() 
{
    cursor = new idx_t(0);
    row_ids = new vector<row_t>;
}

BMHashJoin::~BMHashJoin() 
{
    delete cursor;
    delete row_ids;
}


uint32_t BMHashJoin::reverseBits(uint32_t x)
{
    x = (((x & 0xaaaaaaaa) >> 1) | ((x & 0x55555555) << 1));
    x = (((x & 0xcccccccc) >> 2) | ((x & 0x33333333) << 2));
    x = (((x & 0xf0f0f0f0) >> 4) | ((x & 0x0f0f0f0f) << 4));
    x = (((x & 0xff00ff00) >> 8) | ((x & 0x00ff00ff) << 8));
    return((x >> 16) | (x << 16));
}

void BMHashJoin::util_btv_to_id_list(int64_t *base_ptr, uint32_t &base,
									uint64_t idx, uint32_t bits)
{
#if defined(__AVX512F__)
	// get 31-bit indexes from bits									
	__m512i indexes = _mm512_maskz_compress_epi16(bits, _mm512_set_epi32(
		0x001e001d, 0x001c001b, 0x001a0019, 0x00180017,
		0x00160015, 0x00140013, 0x00120011, 0x0010000f,
		0x000e000d, 0x000c000b, 0x000a0009, 0x00080007,
		0x00060005, 0x00040003, 0x00020001, 0x00000000 
	));
	// Count the number of 1s and obtain the maximum number of required groups
	auto valid_pos_n = _popcnt32(bits);
	auto iter_n = valid_pos_n / 8 + 1;
	// Store the indexes in base_ptr
	__m512i start_index = _mm512_set1_epi64(idx);
	for (int i = 0; i < iter_n; i++) {
		__m128i part;
		if (i == 0) part = _mm512_castsi512_si128(indexes);
		else if (i == 1) part = _mm512_extracti32x4_epi32(indexes, 1);
		else if (i == 2) part = _mm512_extracti32x4_epi32(indexes, 2);
		else part = _mm512_extracti32x4_epi32(indexes, 3);

		__m512i t = _mm512_cvtepu16_epi64(part);

		_mm512_storeu_si512(base_ptr + base + i * 8, _mm512_add_epi64(t, start_index));
	}
	
	base += valid_pos_n;
#else
	const uint16_t src_indices[32] = {
        0x0000, 0x0000, 0x0001, 0x0002, 0x0003, 0x0004, 0x0005, 0x0006, 
		0x0007, 0x0008, 0x0009, 0x000a, 0x000b, 0x000c, 0x000d, 0x000e,
		0x000f, 0x0010, 0x0011, 0x0012, 0x0013, 0x0014, 0x0015, 0x0016,
		0x0017, 0x0018, 0x0019, 0x001a, 0x001b, 0x001c, 0x001d, 0x001e
    };

    uint32_t dst_pos = 0; 
    for (uint32_t i = 0; i < 32; i++) {
        if (bits & (1u << i)) { 
            base_ptr[base + dst_pos] = (int64_t)src_indices[i] + idx;
            dst_pos++;
        }
    }

    base += _popcnt32(bits);
#endif
}

void BMHashJoin::GetRowids(ibis::bitvector &btv_res, std::vector<row_t> *row_ids) {
	row_ids->resize(btv_res.count() + 64);
	auto element_ptr = &(*row_ids)[0];

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

	row_ids->resize(btv_res.count());

	std::cout << "fetch rows : " << row_ids->size() << std::endl;
}

string BMHashJoin::getJoinName(const duckdb::PhysicalHashJoin* comp, string mode) {
    auto NormalizeName = [](string s) {
        auto pos = s.find_last_of('.');
        if (pos != string::npos) s = s.substr(pos + 1);
        s.erase(std::remove_if(s.begin(), s.end(), [](unsigned char c) {
            return std::iscntrl(c) || std::isspace(c);
        }), s.end());
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
        return s;
    };

    if (!comp) return "no_match";

    if (mode == "build") {
        const std::unordered_set<std::string> allowed = {"o_orderkey", "s_suppkey"};
        for (auto &cond : comp->conditions) {
            if (!cond.right) continue;
            auto name = NormalizeName(cond.right->GetName());
            if (allowed.find(name) != allowed.end()) {
                return name;
            }
        }
    }
	else if (mode == "probe") {
		const std::unordered_set<std::string> allowed = {"l_orderkey", "l_suppkey"};
		for (auto &cond : comp->conditions) {
			if (!cond.left) continue;
			auto name = NormalizeName(cond.left->GetName());
			if (allowed.find(name) != allowed.end()) {
				return name;
			}
		}
	}
    return "no_match";
}

}