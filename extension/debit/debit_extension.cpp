#define DUCKDB_EXTENSION_MAIN

#include "debit_extension.hpp"
#include "bm_dbgen.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "utils/util.h"         
#include "CUBIT-dev/src/bitmaps/base_table.h"  
#include "CUBIT-dev/src/bitmaps/rabit/table.h" 
#include "CUBIT-dev/src/fastbit/bitvector.h"  
              

#endif

namespace duckdb {

static void PragmaLoadBitmap(ClientContext &context, const FunctionParameters &parameters) {
    int sf = 10;
    int state = -1;
    /*
    (shipdate, linestatus, returnflag) for Q1(seg_btv)
    (shipdate, shipdate_GE, orderkey) for Q3
    (orderkey) for Q4
    (orderkey) for Q5
    (shipdate_GE_364, discount, quantity) for Q6(seg_btv)
    (orderdate) for Q8(seg_btv)
    (orderkey,returnflag) for Q10
    (o_orderkey) for Q12
    (shipdate_GE_30) for Q14(seg_btv)
    (shipdate_GE_30) for Q15(seg_btv)
    (partkey) for Q17
    (shipinstruct,shipmode) for Q19
    */
    for (const auto &param : parameters.values){
        auto input_value = param.GetValue<string>();

        std::cout << "Loading bitmap: " << input_value << std::endl;
        // FIXME : min_value_EE are not prepared for BTV_GE
        if (input_value == "shipdate") {
            Table_config * config_shipdate = context.Make_Config(input_value, 0, 10562, "bm", 1, false);
            // Table_config * config_shipdate = context.Make_Config(input_value, 10562, "bm", 1, false, false, Index_encoding::GE, 364);
            state = context.Read_BM(config_shipdate, &context.bitmap_shipdate, 59986052);
        }
        else if (input_value == "shipdate_GE") {
            Table_config * config_shipdate_GE = context.Make_Config(input_value, 0, 10562, "bm", 1, false, false, Index_encoding::GE, 364);
            state = context.Read_BM(config_shipdate_GE, &context.bitmap_shipdate_GE, 59986052);
        }
        else if (input_value == "orderdate") {
            Table_config * config_orderdate = context.Make_Config(input_value, 8034, 10440, "bm", 1, true);
            // Table_config * config_orderdate = context.Make_Config(input_value, 10440, "bm", 1, true, false, Index_encoding::GE, 364);
            state = context.Read_BM(config_orderdate, &context.bitmap_orderdate, 59986052);
        } 
        else if (input_value == "discount") {
            Table_config * config_discount = context.Make_Config(input_value, 0, 11, "bm", 1, false);
            state = context.Read_BM(config_discount, &context.bitmap_discount, 59986052);
        } 
        else if (input_value == "quantity") {
            Table_config * config_quantity = context.Make_Config(input_value, 1, 51, "bm", 1, false);
            // Table_config * config = context.Make_Config(input_value, 51, "bm", 1, false, Index_encoding::GE, 10);
            state = context.Read_BM(config_quantity, &context.bitmap_quantity, 59986052);
        } 
        else if (input_value == "custkey") {
            Table_config * config_custkey = context.Make_Config(input_value, 0, 1500001, "bmz", 1000, false);
            state = context.Read_BM(config_custkey, &context.bitmap_custkey, 15000000);
        } 
        else if (input_value == "linestatus") {
            Table_config * config_linestatus = context.Make_Config(input_value, 0, 2, "bm", 1, false);
            state = context.Read_BM(config_linestatus, &context.bitmap_linestatus, 59986052);
        } 
        else if (input_value == "returnflag") {
            Table_config * config_returnflag = context.Make_Config(input_value, 0, 3, "bm", 1, false);
            state = context.Read_BM(config_returnflag, &context.bitmap_returnflag, 59986052);
        }
        else if (input_value == "orderkey") {
            Table_config * config_orderkey = context.Make_Config(input_value, 0, 60000001, "bmz", 6000, false);
            state = context.Read_BM(config_orderkey, &context.bitmap_orderkey, 59986052);
        }
        else if (input_value == "suppkey") {
            Table_config * config_suppkey = context.Make_Config(input_value, 0, 10000 * sf + 1, "bm", 1, false);
            state = context.Read_BM(config_suppkey, &context.bitmap_suppkey, 59986052);
        } 
        else if (input_value == "partkey") {
            Table_config * config_partkey = context.Make_Config(input_value, 0, 200000 * sf + 1, "bm", 1, false);
            state = context.Read_BM(config_partkey, &context.bitmap_partkey, 59986052);
        } 
        else if (input_value == "shipmode") {
            Table_config * config_shipmode = context.Make_Config(input_value, 0, 7, "bm", 1, false);
            state = context.Read_BM(config_shipmode, &context.bitmap_shipmode, 59986052);
        }
        else if (input_value == "shipinstruct") {
            Table_config * config_shipinstruct = context.Make_Config(input_value, 0, 4, "bm", 1, false);
            state = context.Read_BM(config_shipinstruct, &context.bitmap_shipinstruct, 59986052);
        } 
        else if (input_value == "o_orderkey") {
            Table_config * config_o_orderkey = context.Make_Config(input_value, 0, 60000001, "bmz", 6000, false);
            state = context.Read_BM(config_o_orderkey, &context.bitmap_o_orderkey, 15000000);
        }
        else if (input_value == "group2") {
            Table_config * config_group2 = context.Make_Config(input_value, 0, 2, "bm", 1, false);
            state = context.Read_BM(config_group2, &context.bitmap_group1, 59986052);
        }
        else if (input_value == "group8") {
            Table_config * config_group8 = context.Make_Config(input_value, 0, 8, "bm", 1, false);
            state = context.Read_BM(config_group8, &context.bitmap_group2, 59986052);
        }
        // else if (input_value == "receiptdate") {
        //     Table_config * config_receiptdate = context.Make_Config(input_value, 10562, false);
        //     state = context.Read_BM(config_receiptdate, &context.bitmap_receiptdate, 59986052);
        // } 
        else{
            std::cout << "Unknown bitmap name: " << input_value << std::endl;
            continue;
        }

        if (state == 0) {
            std::cout << "Bitmap for " << input_value << " loaded successfully." << std::endl;
            state = -1;
        } else {
            std::cout << "Failed to load bitmap for " << input_value << "." << std::endl;
        }
    }
}

static string PragmaTpchQuery(ClientContext &context, const FunctionParameters &parameters) {
    context.query_source = "bm_tpch";
    auto index = parameters.values[0].GetValue<int32_t>();
    return bmtpch::DBGenWrapper::GetQuery(index);
}

static string PragmaBMGroupBy(ClientContext &context, const FunctionParameters &parameters) {
    context.query_source = "bm_tpch";
    return "SELECT sum(l_quantity) from lineitem WHERE l_shipdate >= CAST('1993-01-01' AS date) AND l_shipdate < CAST('1998-01-01' AS date) group by l_returnflag,l_linestatus;";
}

static void PragmaUse(ClientContext &context, const FunctionParameters &parameters) {
    context.query_source = "use_bitmap";
}

static void PragmaUnuse(ClientContext &context, const FunctionParameters &parameters) {
    context.query_source = "tpch";

}

static void LoadInternal(DuckDB &db) {
    auto &db_instance = *db.instance;

    auto load_bitmap_func = PragmaFunction::PragmaCall("load_bitmap", PragmaLoadBitmap, {LogicalType::VARCHAR}, LogicalType::VARCHAR);
    ExtensionUtil::RegisterFunction(db_instance, load_bitmap_func);

    auto bmtpch_func = PragmaFunction::PragmaCall("bm_tpch", PragmaTpchQuery, {LogicalType::BIGINT});
	ExtensionUtil::RegisterFunction(db_instance, bmtpch_func);

    auto bm_groupby_func = PragmaFunction::PragmaCall("bm_groupby", PragmaBMGroupBy, {}, LogicalType::VARCHAR);
    ExtensionUtil::RegisterFunction(db_instance, bm_groupby_func);

    auto use_func = PragmaFunction::PragmaCall("use_bitmap", PragmaUse, {});
    ExtensionUtil::RegisterFunction(db_instance, use_func);

    auto unuse_func = PragmaFunction::PragmaCall("unuse_bitmap", PragmaUnuse, {});
    ExtensionUtil::RegisterFunction(db_instance, unuse_func);

}

void DebitExtension::Load(DuckDB &db) {
    LoadInternal(db);
}

std::string DebitExtension::Name() {
    return "debit";
}

std::string DebitExtension::Version() const {
    return "1.0.0";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void debit_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    duckdb::LoadInternal(db_wrapper);
}

DUCKDB_EXTENSION_API const char *debit_version() {
    return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif