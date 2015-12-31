module FLenser.PostgreSQL.Provider
open System
open System.Collections.Generic
open System.Data
open System.Text
open System.Data.Common
open FLenser.Core
open Npgsql
open NpgsqlTypes

let create (csb: NpgsqlConnectionStringBuilder) =
    let mutable savepoint = 0
    { new IProvider<_,_,_> with
        member __.Connect() = async {
            let con = new NpgsqlConnection(csb)
            do! con.OpenAsync() |> Async.AwaitTask
            return con }

        member __.CreateParameter(name) = 
            let p = NpgsqlParameter()
            p.ParameterName <- name
            p

        member __.BeginTransaction(con) = con.BeginTransaction()

        member __.Dispose() = ()

        member __.InsertObject(con, tbl, columns) = (fun items -> async {
            if not (Seq.isEmpty items) then
                let sql = 
                    sprintf "copy %s(%s) from stdin" tbl (String.Join (", ", columns))
                use w = con.BeginBinaryImport(sql)
                items |> Seq.iter (fun o ->
                    w.StartRow()
                    for i=0 to Array.length o - 1 do
                        match o.[i] with
                        | null -> w.WriteNull()
                        | o ->
                            match o with
                            | :? bool as x -> w.Write<bool>(x, NpgsqlDbType.Boolean)
                            | :? DateTime as x -> w.Write<DateTime>(x, NpgsqlDbType.Timestamp)
                            | :? float as x -> w.Write<float>(x, NpgsqlDbType.Double)
                            | :? int32 as x -> w.Write<int32>(x, NpgsqlDbType.Integer)
                            | :? int64 as x -> w.Write<int64>(x, NpgsqlDbType.Bigint)
                            | :? String as x -> w.Write<String>(x, NpgsqlDbType.Text)
                            | :? TimeSpan as x -> w.Write<TimeSpan>(x, NpgsqlDbType.Interval)
                            | :? array<byte> as x -> w.Write<byte[]>(x, NpgsqlDbType.Bytea)
                            | t -> failwith (sprintf "unknown type %A" t)) })

        member __.HasNestedTransactions = true
        member __.NestedTransaction(con, t) =
            let name = "savepoint" + savepoint.ToString()
            savepoint <- savepoint + 1
            t.Save(name)
            name
        member __.CommitNested(con, t, s) = savepoint <- savepoint - 1
        member __.RollbackNested(con, t, name) =
            savepoint <- savepoint - 1
            t.Rollback(name) }
