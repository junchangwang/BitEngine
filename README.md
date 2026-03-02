
### Introduction to BitEngine

The database community has long explored bitmap indexing in DBMSs, but most efforts failed to achieve widespread adoption. Traditional bitmap indexes are not general-purpose; they limit to scans on low-cardinality attributes within read-mostly workloads. A notable example is PostgreSQL, which introduced native bitmap indexes in version 8.3.23 but later removed them due to limitations.

In recent years, researchers have proposed innovative bitmap index designs. Recent advances include update-friendly designs such as UpBit [1] and Cubit [2], with RABIT [3] extending support to high-cardinality attributes. For the first time, researchers can build bitmap indexes on attributes of any cardinality (from dozens to millions) in tables ranging from read-only to update-intensive [2,3]. These advances have sparked renewed interest in bitmap indexing, raising an interesting question: beyond traditionally acting as scan accelerators, to what extent can the state-of-the-art bitmap indexes be leveraged in DBMS query engines?

BitEngine answers this by implementing a bitmap index-based query execution engine in DuckDB. At its core is a suite of bitmap-oriented operators for query execution, delivering benefits such as reduced I/O and intermediate data, improved SIMD utilization, and avoidance of costly materializations.


[1] Manos Athanassoulis, Zheng Yan, Stratos Idreos. UpBit: Scalable In-Memory Updatable Bitmap Indexing. In SIGMOD'16.
[2] Junchang Wang, Manos Athanassoulis. CUBIT: Concurrent Updatable Bitmap Indexing. In VLDB'24.
[3] Junchang Wang, Fu Xiao, Manos Athanassoulis. RABIT: Efficient Range Queries with Bitmap Indexing. In SIGMOD'25.





### How to run DEBIT？

First, you need to compile the CUBIT-d under `extension/debit` and the entire project.

```sh
cd extension/debit/CUBIT-d
./build.sh
```

Second，after the compilation is finished, you can load the corresponding bitmap in DuckDB with the following commands：

```DuckDB
pragma load_bitmap(col_name1, col_name2);
```

DEBIT currently supports TPCH Q1, Q5, Q6, and Q14.
For example, if you want to run Q1, you can use the following command in DuckDB:

```DuckDB
set threads to 1;
pragma load_bitmap(shipdate, linestatus, returnflag);
pragma bm_tpch(1);
```
If you want to run the standard DuckDB TPCH queries, use the following command:

```DuckDB
pragma tpch(1);
```

Below are the required bitmap columns for each supported TPCH query in DEBIT:
  
- (shipdate, linestatus, returnflag) for Q1
- (orderkey, suppkey) for Q5
- (shipdate_GE, discount, quantity) for Q6

You can view all the logic of bmquery under `extension/debit/execution/tpch/query`.

### How to download the bitmap files?
You can download the compressed file of bitmap under `https://github.com/junchangwang/Bitmap-dataset.git` and extract it to the root directory of your project for use.The bitmap files are located in the "BITMAPS" folder.