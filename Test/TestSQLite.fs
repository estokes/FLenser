(*  FLenser, a simple ORM for F#
    Copyright (C) 2015 Eric Stokes 

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>. *)
module FLenser.TestSQLite
open System
open System.Data.Common
open System.Collections.Generic
open FSharpx.Control
open System.Data.SQLite
open FLenser.Core

module T1 =
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

    let init =
        Query.Create
            (sprintf "create table foo (%s)" (String.Join(", ", lens.Columns)),
             Lens.NonQuery)

    let items =
       [{item = "foo"; id = 0L; thing = A {foo = 42; bar = "42"; baz = None}}
        {item = "bar"; id = 1L; thing = A {foo = 54; bar = "54"; baz = Some 12.932}}
        {item = "baz"; id = 2L; thing = B {oof = DateTime.Now
                                           rab = Some (TimeSpan.FromSeconds 12.)
                                           zab = set [1;2;3;4]}}
        {item = "zam"; id = 23423429999L; thing = C}
        {item = "zeltch"; id = 3L; thing = D}]

    let byItem = 
        Query.Create("select * from foo where item = :p1", 
            lens, Parameter.String("p1"))

    let changeBar = 
        Query.Create("update foo set thing$a$bar = :p1 where id = :p2", 
            Lens.NonQuery, Parameter.String("p1"), Parameter.Int64("p2"))

module T2 =
    type u = 
        {id: int64
         x: String
         testing: bool}
            [<CreateSubLens>]
            static member Lens(?prefix) =
                // this will cause us not to write the id, but read it as
                // normal. That's what we want since we are going to let
                // the database pick it
                let readid prefix (r: DbDataReader) = 
                    r.GetValue(r.GetOrdinal(prefix + "id"))
                let writeCat (r: u) = box (r.x + r.x)
                Lens.Create<u>(virtualDbFields = Map.ofList ["id", readid],
                               virtualRecordFields = Map.ofList ["cat", writeCat],
                               ?prefix = prefix)

    type t = {a: int; b: float; c: String; thing: u}

    let lens = Lens.Create<t>()

    let init =
        let cols = 
            Array.append lens.Columns [|"thing$id integer primary key autoincrement"|]
            |> fun a -> String.Join(", ", a)
        Query.Create(sprintf "create table bar (%s)" cols, Lens.NonQuery)

    let items =
        [{a = 123; b = 0.2324; c = "bar"; thing = {id = 0L; x = "foo"; testing = false}}
         {a = 42; b = 42.2324; c = "baz"; thing = {id = 0L; x = "rab"; testing = false}}]

    let byCat = 
        Query.Create("select * from bar where thing$cat = :p", lens, Parameter.String("p"))

let joined = 
    Query.Create("select * from foo join bar on foo.id = bar.thing$id", 
        Lens.Tuple(T1.lens, T2.lens))

let cs = SQLiteConnectionStringBuilder("DataSource=:memory:")
cs.JournalMode <- SQLiteJournalModeEnum.Off
cs.CacheSize <- 0

let setupasync () = async {
    let! db = Async.Db.Connect(FLenser.SQLite.Provider.create cs)
    let! _ = db.NonQuery(T1.init, ())
    let! _ = db.NonQuery(T2.init, ())
    do! db.Transaction (fun db -> async {
            do! db.Insert("foo", T1.lens, T1.items) 
            do! db.Insert("bar", T2.lens, T2.items) })
    return db }

let setup () = 
    let db = Db.Connect(FLenser.SQLite.Provider.create cs)
    db.NonQuery(T1.init, ()) |> ignore
    db.NonQuery(T2.init, ()) |> ignore
    db.Transaction(fun db -> 
        db.Insert("foo", T1.lens, T1.items)
        db.Insert("bar", T2.lens, T2.items))
    db

let testSpeed () = 
    let db = setup ()
    for i=0 to 999999 do
        ignore (db.Query(T1.byItem, "bar"))

let testSpeedAsync () = 
    async {
        let! db = setupasync ()
        for i=0 to 999999 do
            do! db.Query(T1.byItem, "bar") |> Async.Ignore 
    } |> Async.RunSynchronously

[<EntryPoint>]
let main argv = 
    let cs = SQLiteConnectionStringBuilder("DataSource=:memory:")
    cs.JournalMode <- SQLiteJournalModeEnum.Off
    cs.CacheSize <- 0
    async {
        let! db = setupasync ()
        let! i1 = db.Query(T1.byItem, "foo")
        if i1.[0] <> List.head T1.items then failwith (sprintf "0: %A" i1.[0])
        let! _ = db.NonQuery(T1.changeBar, ("hello", 0L))
        let! i2 = db.Query(T1.byItem, "foo")
        if i2.[0] = List.head T1.items then failwith (sprintf "1: %A" i2.[0])
        if i2.[0] <> {(List.head T1.items) with 
                            thing = T1.A {foo = 42; bar = "hello"; baz = None}}
        then failwith (sprintf "2: %A" i2.[0])
        let! i3 = db.Query(T2.byCat, "foofoo")
        if i3.[0] <> List.head T2.items then failwith (sprintf "2: %A" i3.[0])
        let! i4 = db.Query(joined, ())
        i4 |> Seq.iter (printf "%A")
    } |> Async.RunSynchronously
    0
