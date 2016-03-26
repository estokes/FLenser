module FLenser.TestPostgreSQL
open System
open Npgsql
open FLenser.Core
open FLenser.Core.Async

let go = Async.RunSynchronously

let connect host db user password =
    let args = NpgsqlConnectionStringBuilder()
    args.Host <- host 
    args.Port <- 5432
    args.SslMode <- SslMode.Require
    args.TrustServerCertificate <- true
    args.Database <- db 
    args.CommandTimeout <- 1200
    args.Username <- user
    args.Password <- password
    let provider = FLenser.PostgreSQL.Provider.create args
    Db.WithRetries(provider, log = (fun e -> printfn "exn %A" e), tries = 3)
    |> go

let db = connect "host" "db" "user" "password"

type test = 
    { id: int
      key: String }

let lens = Lens.Create<test>()

let createTbl = 
    let sql = "create table test(id integer, key text)"
    Query.Create(sql, Lens.NonQuery)

let add =
    let sql = "insert into test (id, key) values (:id, :key)"
    Query.Create(sql, Lens.NonQuery, Parameter.OfLens(lens))

let delete = 
    let sql = "delete from test where id = :id"
    Query.Create(sql, Lens.NonQuery, Parameter.Int("id"))

let find = 
    let sql = "select * from test where id = :id"
    Query.Create(sql, lens, Parameter.Int("id"))

