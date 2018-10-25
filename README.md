# EFContainsBenchmark
Tiny project for benchmarking different approaches to perform Contains on multiple columns using Entity Framework

Idea was to test different approaches to implement the following query in EF as efficiently as possible.

```
var localData = GetDataFromApiOrUser();
var query = from p in context.Prices
            join s in context.Securities on 
              p.SecurityId equals s.SecurityId
            join t in localData  on 
              new { s.Ticker, p.TradedOn, p.PriceSourceId } equals
              new { t.Ticker, t.TradedOn, t.PriceSourceId }
            select p;
var result = query.ToList();
```

The following approaches are used
- Naive: query each element with single query
- Naive with parallelism: same as above, but do in multiple threads
- Multiple contains: query with using .Contains on each used column
- Predicate building: dynamically build Where clause using lots of AND.. AND.. OR
- Shared query table in database: via uploading value for query to the database and query using join to that table
- Using MemoryJoin extension (described [in this article](http://tsherlock.tech/2018/03/20/joining-in-memory-list-to-entity-framework-query/))

* * *
>
> This sample is published as part of the corresponding blog article at https://www.toptal.com/blog
>
> Visit https://www.toptal.com/blog and subscribe to our newsletter to read great posts!
>
> * * *
