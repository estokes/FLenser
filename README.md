# FLenser

A simple object relational mapper for F# algebraic types.

## Installation

NuGet Packages are avaliable. There is a package for the core FLenser.Core, as well
as a package for each database provider, e.g. FLenser.PostgreSQL.

## Example with SQLite

    type a = 
        { foo: int
          bar: String
          baz: Option<float> }

    type b = 
        { oof: DateTime
          rab: Option<TimeSpan>
          zab: Set<int> }

    type z =
        | A of a
        | B of b
        | C
        | D

    type t = 
        { item: String
          id: int64
          thing: z }

    let lens = Lens.Create<t>()

A lens maps an F# algebraic type (record, union, or tuple) to database columns. Lens.Create 
uses reflection to build two functions (held internally) that translate the type to, and from 
the database columns.

    let init =
        Query.Create
            (sprintf "create table foo (%s)" (String.Join(", ", lens.ColumnNames)),
             Lens.NonQuery)

    let cs = SQLiteConnectionStringBuilder("DataSource=:memory:")
    let db = Db.Connect(FLenser.SQLite.Provider.create cs)
    let _ = db.NonQuery(init, ())
    let items =
       [{item = "foo"; id = 0L; thing = A {foo = 42; bar = "42"; baz = None}}
        {item = "bar"; id = 1L; thing = A {foo = 54; bar = "54"; baz = Some 12.932}}
        {item = "baz"; id = 2L; thing = B {oof = DateTime.Now
                                           rab = Some (TimeSpan.FromSeconds 12.)
                                           zab = set [1;2;3;4]}}
        {item = "zam"; id = 23423429999L; thing = C}
        {item = "zeltch"; id = 3L; thing = D}]
    db.Transaction (fun db -> db.Insert("foo", lens, items))
    
Here we create an in memory SQLite database, add a table, and some items to it. Because
SQLite uses manifest typing, we don't need to worry about column types. This won't be true
in other databases. 

    let byItem = 
        Query.Create("select * from foo where item = :p1", 
            lens, Parameter.String("p1"))
    
    let l = db.Query(byItem, "bar")

    val it : List<t> = seq [{item = "bar";
                             id = 1L;
                             thing = A {foo = 54;
                                        bar = "54";
                                        baz = Some 12.932;};}]

Here we create a query to look up objects by the 'item' column, and then execute it. SQLite is
very fast, and while the Lens infrastructure does impose some overhead, the above query runs in
a little under 20us. That's about 20 times slower than a Dictionary lookup, however if you need
to run complex queries on your objects using SQLite as an in memory store could save you quite a lot
of work. Also, since objects in SQLite are outside the managed heap, the garbage collector won't
look at them anymore, which is potentially very useful for infrequently used objects.

