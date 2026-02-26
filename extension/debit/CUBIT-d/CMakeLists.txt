cmake_minimum_required(VERSION 3.16.0)

option(test "Build all tests." OFF) # Makes boolean 'test' available.

set(LIB_NAME bitmap)
set(PROJECT_NAME nicolas)

project(${PROJECT_NAME})

include_directories(${CMAKE_SOURCE_DIR}/src)

# Linux specific code
IF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    # RABIT relies on RCU to lazily free nodes. The RCU mechanism, however, incurs the invalid-offsetof warning. So we disable it.
    # The fastbit lib incurs a deprecated-declarations warning. So we disable it since we don't want to touch fastbit's code anyway.
    set(CMAKE_CXX_FLAGS "-g -ggdb -O3 -std=gnu++17 -fPIC -Wno-invalid-offsetof -Wno-deprecated-declarations")
    # set(CMAKE_CXX_FLAGS "-g -O3 -std=gnu++17 -fPIC")

    include_directories("/usr/include/x86_64-linux-gnu")
    include_directories("/usr/local/include")
    ADD_DEFINITIONS( "-DLINUX" )
    set(BOOST_LIBRARYDIR /usr/lib/x86_64-linux-gnu)
    set(BOOST_INCLUDEDIR /usr/include)
    set(Boost_NO_SYSTEM_PATHS on)
    find_package(Boost COMPONENTS filesystem program_options system REQUIRED)
ENDIF(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

find_library(URCU_LIBRARIES 
                NAMES urcu
                HINTS "/usr/lib/x86_64-linux-gnu;/usr/local/lib")

# fastbit
set(FASTBIT_OBJ src/fastbit/bitvector_base.h
                  src/fastbit/bitvector.cpp src/fastbit/bitvector.h
                  src/fastbit/bitvector16.cpp src/fastbit/bitvector16.h
                  src/fastbit/bitvector8.cpp src/fastbit/bitvector8.h
                  src/fastbit/fileManager.cpp src/fastbit/fileManager.h
                  src/fastbit/resource.cpp src/fastbit/resource.h
                  src/fastbit/util.cpp src/fastbit/util.h
                  src/fastbit/array_t.cpp src/fastbit/array_t.h)

# bitmap
set(RABIT_OBJ src/bitmaps/rabit/segBtv.h src/bitmaps/rabit/segBtv.cpp 
        src/bitmaps/rabit/table.h src/bitmaps/rabit/table.cpp)
set(UTIL_OBJ src/utils/util.h src/utils/util.cpp)
add_library(bitmap STATIC ${FASTBIT_OBJ} ${RABIT_OBJ} ${UTIL_OBJ})
target_link_libraries(bitmap pthread atomic ${Boost_LIBRARIES} ${URCU_LIBRARIES})
