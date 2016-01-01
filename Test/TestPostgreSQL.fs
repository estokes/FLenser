module FLenser.TestPostgreSQL
open System
open Npgsql
open FLenser.Core
open FLenser.Core.Async

type agent = 
    { id: int
      firstName: String
      lastName: String
      companyName: String
      email: String
      phone: String }
    [<CreateSubLens>]
    static member Lens(?prefix) =
        let pfx = defaultArg prefix ""
        let id : virtualDbField = 
            fun prefix r -> 
                let ord = r.GetOrdinal (prefix + "id")
                box (r.GetInt32 ord)
        Lens.Create<agent>(?prefix = prefix, 
            virtualDbFields = Map.ofList ["id", id])
    static member Empty =
        { id = 0; firstName = ""; lastName = ""; companyName = ""
          email = ""; phone = "" }

let connect host db password =
    let args = NpgsqlConnectionStringBuilder()
    args.Host <- host 
    args.Port <- 5432
    args.SslMode <- SslMode.Require
    args.TrustServerCertificate <- true
    args.Database <- db 
    args.CommandTimeout <- 1200
    args.Username <- "estokes"
    args.Password <- password
    let provider = FLenser.PostgreSQL.Provider.create args
    Db.WithRetries(provider, log = (fun e -> printfn "exn %A" e), tries = 3)
    |> Async.RunSynchronously

let db = connect "host" "db" "password"

let lens = agent.Lens()

let testAgent = 
    { id = 0
      firstName = "Test"
      lastName = "Agent"
      companyName = "Test Firm"
      email = "test@testfirm.com"
      phone = "5555555555" }

let () = 
    db.Insert("agents", lens, [testAgent])
    |> Async.RunSynchronously

let allAgents = Query.Create("select * from agents", lens)
let agents = db.Query(allAgents, ()) |> Async.RunSynchronously

let removeAgent = 
    Query.Create("delete from agents where id = :id", Lens.NonQuery,
        Parameter.Int("id"))
