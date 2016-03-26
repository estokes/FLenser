# FLenser

A simple object relational mapper for F# algebraic types.

Type providers and query expressions can be great, however FLenser is for you if,

* You don't want the complexity of setting up type providers in your dev/prod environment
* You want to work directly with F# algebraic types instead of traditional objects
* You don't mind (or prefer) using SQL directly

## Installation

NuGet Packages are avaliable. There is a package for the core FLenser.Core, as well
as a package for each database provider, e.g. FLenser.PostgreSQL.

## Recent Changes
1.4.12 and 13
* Just bug fixes

1.4.11 
* Default nesting separator changed to "" for parameters and lenses
* Parameter.OfLens now allows specifying a prefix
* Behavior of Flatten changed to respect the root prefix

1.4
* Lenses may now be used as parameters, which for one greatly simplifies large update
statements, and also opens up many structured query possibilities.
* prefix, and map keys of virtual fields are now string lists, each element representing
a token that will be separated by the nesting separator internally. This simplifies both
the internal and external handling of nesting.

## Example with SQLite

    open System
    open System.Data.Common
    open System.Data.SQLite
    open System.Collections.Generic
    open FLenser.Core

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
a little under 16us. That's about 16 times slower than a Dictionary lookup, however if you need
to run complex queries on your objects using SQLite as an in memory store could save you quite a lot
of work. Also, since objects in SQLite are outside the managed heap, the garbage collector won't
look at them anymore, which is potentially very useful for infrequently used objects.

