
### How to run DEBIT？

First, you need to add the CUBIT-dev submodule under `extension/debit` with the `RABIT` branch.

```sh
cd extension/debit
git submodule add -b RABIT https://github.com/junchangwang/CUBIT-dev.git
git submodule update --init --recursive
```

Second，after the compilation is finished, you can load the corresponding bitmap in DuckDB with the following commands：

```DuckDB
pragma load_bitmap(col_name1, col_name2);
```

DEBIT currently supports TPCH Q1, Q5, Q6, and Q14.
For example, if you want to run Q6, you can use the following command in DuckDB:

```DuckDB
pragma load_bitmap(shipdate,discount,quantity);
pragma bm_tpch(6);
```
If you want to run the standard DuckDB TPCH queries, use the following command:

```DuckDB
pragma tpch(6);
```

Below are the required bitmap columns for each supported TPCH query in DEBIT:
  
- (shipdate, linestatus, returnflag) for Q1
- (orderdate_GE_364) for Q5
- (shipdate_GE_364, discount, quantity) for Q6
- (shipdate_GE_30) for Q14
