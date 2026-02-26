#include <urcu.h>
#include <chrono>
#include <shared_mutex>

#include "fastbit/bitvector.h"
#include "bitmaps/base_table.h"
#include "bitmaps/rabit/table.h"

using namespace std;

uint64_t db_timestamp __attribute__((aligned(128)));
uint64_t db_number_of_rows;