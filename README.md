# FLenser
A lightweight object relational mapper for F# record types.

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

A lens maps an F# record type to database columns. Lens.Create uses reflection 
to build two functions (held internally) that translate the type to, and from the 
database columns.

    let init =
        Query.Create
            ("""create table foo 
                (item text not null, 
                 id bigint not null, 
                 thing int not null, 
                 thing$a$foo int, 
                 thing$a$bar text, 
                 thing$a$baz double precision,
                 thing$b$oof timestamp,
                 thing$b$rab interval,
                 thing$b$zab json)""", 
             Lens.NonQuery)

    let cs = SQLiteConnectionStringBuilder("DataSource=:memory:")
    let! db = Db.Connect(FLenser.SQLite.Provider.create cs)
    let! _ = db.NonQuery(init, ())
    let items =
       [{item = "foo"; id = 0L; thing = A {foo = 42; bar = "42"; baz = None}}
        {item = "bar"; id = 1L; thing = A {foo = 54; bar = "54"; baz = Some 12.932}}
        {item = "baz"; id = 2L; thing = B {oof = DateTime.Now
                                           rab = Some (TimeSpan.FromSeconds 12.)
                                           zab = set [1;2;3;4]}}
        {item = "zam"; id = 23423429999L; thing = C}
        {item = "zeltch"; id = 3L; thing = D}]
    do! db.Transaction (fun db -> db.Insert("foo", lens, items))
    
We can then create an in memory SQLite database, and add a table to it that corresponds 