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
        member __.ConnectAsync() = async {
            let con = new NpgsqlConnection(csb)
            do! con.OpenAsync() |> Async.AwaitTask
            return con }

        member __.Connect() =
            let con = new NpgsqlConnection(csb)
            con.Open()
            con

        member __.CreateParameter(name) = 
            let p = NpgsqlParameter()
            p.ParameterName <- name
            p

        member __.BeginTransaction(con) = con.BeginTransaction()

        member __.Dispose() = ()

        member __.InsertObject(con, tbl, columns) = (fun items -> 
            if not (Seq.isEmpty items) then
                let sql = 
                    sprintf "COPY %s (%s) FROM STDIN BINARY" tbl (String.Join (", ", columns))
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
                            | t -> failwith (sprintf "unknown type %A" t)))

        member p.InsertObjectAsync(con, tbl, columns) = 
            let f = p.InsertObject(con, tbl, columns)
            (fun items -> async {
                let! res = Async.StartChild(async {
                    do! Async.SwitchToThreadPool()
                    f items })
                return! res })

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
