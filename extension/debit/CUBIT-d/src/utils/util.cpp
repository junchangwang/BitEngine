#include <urcu.h>

#include "fastbit/bitvector.h"
#include "bitmaps/base_table.h"
#include "utils/util.h"
#include "bitmaps/ub/table.h"
#include "bitmaps/ucb/table.h"
#include "bitmaps/naive/table.h"
#include "bitmaps/rabit/table.h"
#define CPU_GHZ 2.0
#define N_MAX_WORKERS (256)
// #define HZ (3.0) //FIXME: only for the SkyLake server with Intel(R) Xeon(R) Gold 5117 CPU @ 2.00GHz.

using namespace std;

struct RABIT_ThreadInfo g_ths_info[N_MAX_WORKERS];

// Export this variable to bitvector. This is a shortcut fix. 
// Bitvector should use the version in Table_config in future.
bool enable_fence_pointer = false;

double time_diff(struct timeval x, struct timeval y) {
    double x_ms , y_ms , diff;
    x_ms = (double) x.tv_sec * 1000000 + (double) x.tv_usec;
    y_ms = (double) y.tv_sec * 1000000 + (double) y.tv_usec;
    diff = y_ms - x_ms;
    return diff;
}

// Receive two HZ numbers, and return the difference in nanoseconds.
long rdtsc_diff(long before, long after)
{
    if (after <= before)
        return 0;
    return (after-before) / (CPU_GHZ);
}

int _CAS2(volatile long * ptr, long * cmp1, long * cmp2, long val1, long val2)
{
  char success;
  long tmp1 = *cmp1;
  long tmp2 = *cmp2;

  __asm__ __volatile__(
      "lock cmpxchg16b %1\n"
      "setz %0"
      : "=q" (success), "+m" (*ptr), "+a" (tmp1), "+d" (tmp2)
      : "b" (val1), "c" (val2)
      : "cc" );

  *cmp1 = tmp1;
  *cmp2 = tmp2;
  return success;
}

po::variables_map get_options(const int argc, const char *argv[]) {
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
            /* Algorithm Configuration */
            ("approach", po::value<string>()->default_value(std::string("ub")), "naive, ucb, ub, rabit")
            ("mode", po::value<string>(), "build / query(mix) / range")
            ("encoding-scheme", po::value<string>()->default_value("EE"), "EE / RE / AE / IE / GE")
            ("data-path", po::value<string>(), "data file path")
            ("index-path", po::value<string>(), "index file path")
            ("group-path", po::value<string>(), "group file path")
            ("number-of-rows", po::value<uint64_t>(), "number of rows")
            ("cardinality", boost::program_options::value<int32_t >()->default_value(1), "cardinality")
            ("ee-range", po::value<int32_t>()->default_value(0), "EE range")
            ("re-range", po::value<int32_t>()->default_value(0), "RE range")
            /* Workloads */
            ("workers",po::value<uint32_t>(), "number of concurrent workers")
            ("number-of-queries", po::value<uint32_t>(), "number of queries")
            ("number-of-udis", po::value<uint32_t>(), "number of UDIs")
            /* Parallel */
            ("merge-threshold",po::value<uint32_t>()->default_value(128), "merge threshold for both UpBit and Rabit")
            ("merge-threads",po::value<uint32_t>()->default_value(4), "number of merge threads")
            ("helper-threads", po::value<unsigned int>()->default_value(8), "num of threads for parallelized get_value/bitmap_init/segbtv_cnt")
            ("cnt-threads", po::value<bool>()->default_value(false), "enable segbtv_cnt")
            /* Segmented Btv */
            ("rows-per-seg", po::value<uint64_t>(), "Rows per segment")
            /* Range Query*/
            ("RQ-length", po::value<uint32_t>()->default_value(0), "length of range queries")
            /* GE related */  
            ("GE-group-len", po::value<int>(), "GE group length")
            /* Helper Functions */
            ("show-memory", po::value<bool>()->default_value(false), "show memory")
            ("help", "produce help message")
            ("verbose", po::value<bool>()->default_value(true), "verbose")
            ("perf", po::value<bool>()->default_value(false), "enable perf")
            /* Fence Pointer */
            ("fence-pointer", po::value<bool>()->default_value(false), "enable fence pointers")
            ("fence-length", po::value<unsigned int>()->default_value(1000), "lengh of fence pointers")
            /* Retired */
            ("on-disk", po::value<bool>()->default_value(false), "on disk");
            

    po::positional_options_description p;
    p.add("mode", -1);

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        cout << desc << endl;
        exit(1);
    }

    return vm;
}

void init_config(Table_config *config, po::variables_map options) {
    config->n_workers = options.count("workers") ?
                    options["workers"].as<uint32_t>() : 1;
    if (options.count("data-path"))
        config->DATA_PATH = options["data-path"].as<string>();
    if (options.count("group-path"))
        config->GROUP_PATH = options["group-path"].as<string>();
    if (options.count("cardinality"))
        config->g_cardinality = options["cardinality"].as<int32_t>();
    if (options.count("ee-range"))
        config->ee_range = options["ee-range"].as<int32_t>();
    if (options.count("re-range"))
        config->re_range = options["re-range"].as<int32_t>();
    if (options.count("index-path"))
        config->INDEX_PATH = options["index-path"].as<string>();
    if (options.count("number-of-rows"))
        config->n_rows = options["number-of-rows"].as<uint64_t>();
    if (options.count("number-of-udis"))
        config->n_udis = options["number-of-udis"].as<uint32_t>();
    if (options.count("number-of-queries"))
        config->n_queries = options["number-of-queries"].as<uint32_t>();
    config->verbose = options["verbose"].as<bool>();
    config->enable_fence_pointer = options["fence-pointer"].as<bool>();
    config->enable_parallel_cnt = options["cnt-threads"].as<bool>();
    enable_fence_pointer = config->enable_fence_pointer;
    config->show_memory = options["show-memory"].as<bool>();
    config->on_disk = options["on-disk"].as<bool>();
    config->approach = options["approach"].as<string>();
    // This variable is defind in fastbit/bitvector.cpp
    // Ideally, each bitmap must have its own INDEX_WORDS.
    // But for simplicity, we assume they use the same configuration here.
    INDEX_WORDS = options["fence-length"].as<unsigned int>();
    config->nThreads_for_getval = options["helper-threads"].as<unsigned int>();
    config->nThreads_for_merge = options["merge-threads"].as<uint32_t>();
    config->n_range = options["RQ-length"].as<uint32_t>();

    config->showEB = false;
    config->decode = false;
    config->autoCommit = true;
    config->n_merge_threshold = options["merge-threshold"].as<uint32_t>();
    config->db_control = false;

    // For encoding schemes
    config->encoding = EE;

    if (options.count("GE-group-len")) {
        config->GE_group_len = options["GE-group-len"].as<int>();
    }
    
    // For segmented bitvectors
    config->encoded_word_len = 31;
    if (options.count("rows-per-seg") && (config->approach == "cubit" || config->approach == "rabit"))
        config->segmented_btv = true;
    else
        config->segmented_btv = false;
    if (config->segmented_btv) {
        config->rows_per_seg = options["rows-per-seg"].as<uint64_t>();
        cout << "=== Using Segmented Btv. Row per seg: " << config->rows_per_seg << endl;
    }
}
