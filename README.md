
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
pragma load_bitmap(shipdate, linestatus, returnflag);
pragma bm_tpch(1);
```
If you want to run the standard DuckDB TPCH queries, use the following command:

```DuckDB
pragma tpch(1);
```

Below are the required bitmap columns for each supported TPCH query in DEBIT:
  
- (shipdate, linestatus, returnflag) for Q1
- (orderkey) for Q5
- (shipdate_GE_364, discount, quantity) for Q6
- (shipdate_GE_30) for Q14

You can view all the logic of bmquery under `extension/debit/execution/tpch/query`.