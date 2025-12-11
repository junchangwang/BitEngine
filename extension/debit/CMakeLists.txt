cmake_minimum_required(VERSION 3.16.0)

project(DebitExtension)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../src/include/)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/CUBIT-dev/src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/dbgen/include)


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++17")

add_compile_options(-mavx512f -march=native)

set(FILES 
    ${CMAKE_CURRENT_SOURCE_DIR}/debit_extension.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/CUBIT-dev/src/utils/util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/CUBIT-dev/src/bitmaps/base_table.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/CUBIT-dev/src/bitmaps/rabit/table.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/CUBIT-dev/src/fastbit/bitvector.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dbgen/bm_dbgen.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dbgen/bmtpch_constants.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/bitmap_table_scan.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q1.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q3.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q4.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q5.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q6.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q8.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q10.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q12.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q14.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q15.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q17.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/query/Q19.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/test/DebitGather.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/test/DebitSIMD.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/test/DuckdbSIMD.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/test/groupbyTest.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/execution/tpch/test/hashjoin.cpp
    
)

build_static_extension(debit ${FILES})

build_loadable_extension(debit "" ${FILES})

#target_link_libraries(debit_extension pthread atomic ${Boost_LIBRARIES})

install(
  TARGETS debit_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)