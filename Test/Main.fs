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
open FSharpx.Control
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

type Sequencer() =
    let mutable stop = false
    let mutable available = AsyncResultCell()
    let jobs = Queue<Async<Unit>>()
    let rec loop () = async {
        if stop then ()
        else
            do! available.AsyncResult
            let job =
                lock jobs (fun () -> 
                    if jobs.Count = 0 then
                        available <- AsyncResultCell()
                        None
                    else Some (jobs.Dequeue ()))
            match job with
            | None -> return! loop ()
            | Some job ->
                do! job 
                return! loop () }
    do loop () |> Async.StartImmediate
    interface IDisposable with member __.Dispose() = stop <- true
    member __.Enqueue(job : Async<'A>) : Async<'A> =
        if stop then failwith "The sequencer is stopped!"
        let res = AsyncResultCell()
        lock jobs (fun () ->
            if jobs.Count = 0 then available.RegisterResult (AsyncOk ())
            let job = async {
                try
                    let! z = job
                    res.RegisterResult (AsyncOk z)
                with e -> 
                    return res.RegisterResult (AsyncException e) }
            jobs.Enqueue(job))
        res.AsyncResult

let testSeq () =
    let rnd = Random()
    use seq = new Sequencer()
    let mutable num = 0
    async {
        do! [1..10000]
            |> Seq.map (fun _ -> 
                seq.Enqueue(async {
                    num <- num + 1
                    do! Async.Sleep (rnd.Next(1, 5))
                    num <- num - 1
                    if num <> 0 then printfn "oops %d" num }))
            |> Async.Parallel
            |> Async.Ignore
    } |> Async.RunSynchronously

type Throttle(maxConcurrentJobs: int, start: Async<_> -> unit) = 
    let waiting = BlockingQueueAgent<_>(max maxConcurrentJobs 50)
    let running = BlockingQueueAgent<_>(maxConcurrentJobs)
    let rec loop () = async {
        let! (job, finished) = waiting.AsyncGet ()
        do! running.AsyncAdd (())
        start job
        async { do! finished
                do! running.AsyncGet () }
        |> Async.StartImmediate
        return! loop () }
    do loop () |> Async.Start
    member t.EnqueueWait(job: Async<'A>) : Async<'A> = async { 
        let! res = t.Enqueue job
        return! res }
    member __.Enqueue(job: Async<'A>) : Async<Async<'A>> = async {
        let result = AsyncResultCell()
        let job = async {
            try
                let! res = job
                result.RegisterResult (AsyncOk res)
            with e -> result.RegisterResult (AsyncException e) }
        let finished = async {
            try do! result.AsyncResult |> Async.Ignore
            with _ -> () }
        do! waiting.AsyncAdd ((job, finished))
        return result.AsyncResult }


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

System.GC.Collect(2, System.GCCollectionMode.Forced, true);;

let th = Throttle(1, Async.StartImmediate)
let testThrottle () =
    let db = setup ()
    async {
        for i=0 to 999999 do
            do! th.EnqueueWait(async { ignore (db.Query(byItem, "bar") : List<t>)})
    } |> Async.RunSynchronously

let testAsync () =
    let db = setup ()
    async {
        for i=0 to 999999 do
            let res = AsyncResultCell()
            async { res.RegisterResult(AsyncOk (ignore (db.Query(byItem, "bar")))) }
            |> Async.StartImmediate
            do! res.AsyncResult
    } |> Async.RunSynchronously

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
