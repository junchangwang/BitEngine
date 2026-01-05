#include "execution/tpch/bitmap_table_scan.hpp"


namespace duckdb {

unsigned char reverse_table[256] = {
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

BMTableScan::BMTableScan()
{
    cursor = new idx_t(0);
    row_ids = new vector<row_t>;
}

BMTableScan::~BMTableScan()
{
    delete row_ids;
    delete cursor;
}

uint32_t BMTableScan::reverseBits(uint32_t x)
{
    x = (((x & 0xaaaaaaaa) >> 1) | ((x & 0x55555555) << 1));
    x = (((x & 0xcccccccc) >> 2) | ((x & 0x33333333) << 2));
    x = (((x & 0xf0f0f0f0) >> 4) | ((x & 0x0f0f0f0f) << 4));
    x = (((x & 0xff00ff00) >> 8) | ((x & 0x00ff00ff) << 8));
    return((x >> 16) | (x << 16));
}

void BMTableScan::util_btv_to_id_list(int64_t *base_ptr, uint32_t &base,
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

void BMTableScan::GetRowids(ibis::bitvector &btv_res, std::vector<row_t> *row_ids) {
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

void BMTableScan::GetRowidsSeg(SegBtv &btv_res, vector<row_t> *row_ids)
{

    // count the number of ones in btv_res
    uint32_t count = 0;
    for(const auto & [id_t, seg_1] : btv_res.seg_table) {
        count += seg_1->btv->cnt();
    }
    row_ids->resize(count + 64 + 32);
    auto element_ptr = &(*row_ids)[0];

    uint32_t ids_count = 0;
    uint64_t ids_idx = 0;
    // traverse every segment
    for(const auto & [id_t, seg_1] : btv_res.seg_table) {
        auto it = seg_1->btv->m_vec.begin();
        while(it != seg_1->btv->m_vec.end()) {
            util_btv_to_id_list(element_ptr, ids_count, ids_idx, reverseBits(*it));
            ids_idx += 31;
            it++;
        }

        // active word
        util_btv_to_id_list(element_ptr, ids_count, ids_idx, \
                                reverseBits(seg_1->btv->active.val << (31 - seg_1->btv->active.nbits)));
        ids_idx += seg_1->btv->active.nbits;

    }
    row_ids->resize(count);

    std::cout << "fetch rows : " << row_ids->size() << std::endl;
}

void BMTableScan::btv_logic_or(ibis::bitvector *res, ibis::bitvector *rhs)
{
	// assume res.size() == rhs.size()
	// assume both uncompress
	size_t mvec_size = res->m_vec.size();
	size_t i = 0;
	// __m512i a,b,c;
	ibis::array_t<ibis::bitvector::word_t>::iterator i0 = res->m_vec.begin();
	ibis::array_t<ibis::bitvector::word_t>::const_iterator i1 = rhs->m_vec.begin();
	ibis::array_t<ibis::bitvector::word_t>::iterator iend = res->m_vec.begin() + (res->m_vec.size() / 16) * 16;
	__m512i *srcp = (__m512i *)i0;
	__m512i *dstp = (__m512i *)i1;
	__m512i *t = (__m512i *)iend;
	while(srcp < t) {
		const __m512i a = _mm512_loadu_si512(srcp);
		const __m512i b = _mm512_loadu_si512(dstp++);
		const __m512i c = _mm512_or_epi32(a, b);
		_mm512_storeu_si512(srcp++, c);
	}
}

void BMTableScan::bm_gather_i64_from_i32_idx(const int64_t* A, const uint32_t* B, int n, int64_t* out) {
    int i = 0;
    for (; i + 8 <= n; i += 8) {
        __m256i idx = _mm256_loadu_si256((const __m256i*)(B + i)); // 8 x u32
        __m512i gathered = _mm512_i32gather_epi64(idx, (const void*)A, 8);
        _mm512_storeu_si512((void*)(out + i), gathered);
    }
    // tail
    int rem = n - i;
    if (rem) {
        for (int j = 0; j < rem; ++j) {
            out[i + j] = A[B[i + j]];
        }
    }
}

void BMTableScan::bm_exe_aggregation(int64_t *price_ptr, int64_t *discount_ptr, uint16_t base, int64_t &sum) 
{
#if defined(__AVX512F__)

	__m512i discount = _mm512_loadu_epi64(discount_ptr + base);
	__m512i price = _mm512_loadu_epi64(price_ptr + base);
	__m512i price_times_discount = _mm512_mullo_epi64(price, discount);
	sum += _mm512_reduce_add_epi64(price_times_discount);
#else
    int64_t total = 0;
    for (int i = 0; i < 8; ++i) {
        total += int64_t(price_ptr[base + i]) * int64_t(discount_ptr[base + i]);
    }
    sum += total;		
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

vector<uint32_t>* BMTableScan::reduce_leadingbits(ibis::bitvector &ttt_res) {
    size_t total_bits = ttt_res.size();
    vector<uint32_t> *btv_res = new vector<uint32_t>(total_bits / 32 + (total_bits % 32 ? 1 : 0) + 4);

    uint32_t *dst_data = (uint32_t *)&(*btv_res)[0];
    uint32_t *src_data = ttt_res.m_vec.begin();
    uint32_t* src_end = ttt_res.m_vec.end();

    // load 31 dst once
    while(src_data + 31 < src_end) {
        reduce_zero(dst_data, src_data);
        dst_data += 31;
        src_data += 32;
    }
    // 1 dst once
    int need_bits = 31;
    while(src_data + 1 < src_end) {
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
    } else {
        dst_data[0] |= (last_word >> need_bits);
        dst_data[0] = htonl(dst_data[0]);
    }
    return btv_res;
}

vector<uint32_t>* BMTableScan::reduce_leadingbits_seg(SegBtv &ttt_res) {
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

    return btv_res;
}


}