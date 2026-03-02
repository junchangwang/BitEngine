
#### Introduction to BitQ

BitEngine is a bitmap-oriented query engine that leverages bitmap indexing to accelerate operators (see the main branch for details). BitQ embodies the implementation of BitEngine in analytical DBMSs (DuckDB).


#### Code organization

The code of BitQ is organized in the `extension/debit/execution/operator` directory, which contains the implementation of various operators, including scan, group by, and join.


#### How to run BitQ?

1) Compile the code, generate TPC-H workloads and bitmap instances as in the main branch.

2) Activate BitQ via the following command:

```DuckDB
set threads to 1;
pragma use_bitmap;
```

3) Load the required bitmaps, as in the main branch. Then you can execute BitQ using the following commands:

```DuckDB
pragma load_bitmap(shipdate,discount,quantity);
pragma tpch(6);
```

The above command will direct the query engine to select BitOperators, when possible, to execute the query.
