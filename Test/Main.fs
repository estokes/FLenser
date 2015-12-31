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
module FLenser.Test
open System
open System.Collections.Generic
open System.Data.SQLite
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

let init =
    Query.Create
        (sprintf "create table foo (%s)" (String.Join(", ", lens.ColumnNames)),
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

let raw () = 
    let cs = SQLiteConnectionStringBuilder("DataSource=:memory:")
    cs.JournalMode <- SQLiteJournalModeEnum.Off
    cs.CacheSize <- 0
    let con = new SQLiteConnection(cs.ConnectionString)
    con.Open()
    let cmd = new SQLiteCommand("create table foo (foo, bar, baz)", con)
    cmd.ExecuteNonQuery() |> ignore
    for i = 0 to 10 do
        let cmd = new SQLiteCommand(sprintf "insert into foo values (%d, %s, %g)" i (i.ToString ()) (float i), con)
        cmd.ExecuteNonQuery() |> ignore
    let cmd = new SQLiteCommand("select * from foo where bar = :p", con)
    let p = SQLiteParameter("p")
    cmd.Parameters.AddRange [|p|]
    cmd.Prepare()
    for i = 0 to 999999 do
        p.Value <- 8
        use r = cmd.ExecuteReader() 
        r.Read() |> ignore
        let o = r.GetValues()
        if i % 1000 = 0 then printfn "%A" o
        r.Close()
    con

let cs = SQLiteConnectionStringBuilder("DataSource=:memory:")
cs.JournalMode <- SQLiteJournalModeEnum.Off
cs.CacheSize <- 0

let setupasync () = async {
    let! db = Db.ConnectAsync(FLenser.SQLite.Provider.create cs)
    let! _ = db.NonQuery(init, ())
    do! db.Transaction (fun db -> db.Insert("foo", lens, items))
    return db }

let setup () = 
    let db = Db.Connect(FLenser.SQLite.Provider.create cs)
    db.NonQuery(init, ()) |> ignore
    db.Transaction(fun db -> db.Insert("foo", lens, items))
    db

[<EntryPoint>]
let main argv = 
    let cs = SQLiteConnectionStringBuilder("DataSource=:memory:")
    cs.JournalMode <- SQLiteJournalModeEnum.Off
    cs.CacheSize <- 0
    async {
        let! db = setupasync ()
        let! i1 = db.Query(byItem, "foo")
        if i1.[0] <> List.head items then failwith (sprintf "0: %A" i1.[0])
        let! _ = db.NonQuery(changeBar, ("hello", 0L))
        let! i2 = db.Query(byItem, "foo")
        if i2.[0] = List.head items then failwith (sprintf "1: %A" i2.[0])
        if i2.[0] <> {(List.head items) with 
                            thing = A {foo = 42; bar = "hello"; baz = None}}
        then failwith (sprintf "2: %A" i2.[0])
    } |> Async.RunSynchronously
    0
