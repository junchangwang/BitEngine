### What is BitQ?
BitQ is an implementation that runs bitmap queries by modifying or replacing the operators in duckdb.
We mainly replaced the three operators: table scan, group by, and join. The specific implementation code can be found under `extension/debit/execution/operator`.
### How to run BitQ？

First, you need to compile the CUBIT-d under extension/debit and the entire project.

```sh
cd extension/debit/CUBIT-d
./build.sh
```

Second，after the compilation is finished, you can load the corresponding bitmap in DuckDB with the following commands：

```DuckDB
pragma load_bitmap(col_name1, col_name2);
```

BitQ needs to be activated through a command. Once activated, when you import the required bitmap, you can successfully execute the corresponding bitmap query.
For example, if you want to run Q6, you can use the following command in DuckDB:

```DuckDB
set threads to 1;
pragma use_bitmap;
pragma load_bitmap(shipdate,discount,quantity);
pragma tpch(6);
```

