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
    { new Provider<_,_,_> with
        member __.ConnectAsync() = async {
            let con = new NpgsqlConnection(csb)
            do! con.OpenAsync() |> Async.AwaitTask
            return con }

        member __.Connect() =
            let con = new NpgsqlConnection(csb)
            con.Open()
            con

        member __.CreateParameter(name, typ) =
            // this is much slower than matching with :? which is why
            // we don't combine the two mappings of pgtypes
            let pgtyp =
                if typ = typeof<bool> then NpgsqlDbType.Boolean
                elif typ = typeof<DateTime> then NpgsqlDbType.Timestamp
                elif typ = typeof<float> then NpgsqlDbType.Double
                elif typ = typeof<int> then NpgsqlDbType.Integer
                elif typ = typeof<int64> then NpgsqlDbType.Bigint
                elif typ = typeof<String> then NpgsqlDbType.Text
                elif typ = typeof<TimeSpan> then NpgsqlDbType.Interval
                elif typ = typeof<byte> then NpgsqlDbType.Bytea
                else NpgsqlDbType.Unknown
            NpgsqlParameter(name, pgtyp)

        member __.BeginTransaction(con) = con.BeginTransaction()

        member __.Dispose() = ()

        member __.PrepareInsert(con, tbl, columns) =
            let w : ref<Option<NpgsqlBinaryImporter>> = ref None
            {new PreparedInsert with
                member t.Dispose() = 
                    lock w (fun () -> 
                        match !w with
                        | None -> ()
                        | Some o -> t.Finish())
                
                member __.Finish() = 
                    lock w (fun () -> 
                        match !w with
                        | None -> failwith "no write in progress!"
                        | Some o -> 
                            w := None
                            o.Close ())
                
                member __.Row(o) =
                    lock w (fun () -> 
                        let w =
                            match !w with
                            | Some w -> w
                            | None ->
                                let sql = 
                                    sprintf "COPY %s (%s) FROM STDIN BINARY" tbl (String.Join (", ", columns))
                                let b = con.BeginBinaryImport(sql)
                                w := Some b
                                b
                        w.StartRow()
                        for i=0 to o.Length - 1 do
                            match o.[i] with
                            | null -> w.WriteNull()
                            | :? bool as x -> w.Write<bool>(x, NpgsqlDbType.Boolean)
                            | :? DateTime as x -> w.Write<DateTime>(x, NpgsqlDbType.Timestamp)
                            | :? float as x -> w.Write<float>(x, NpgsqlDbType.Double)
                            | :? int32 as x -> w.Write<int32>(x, NpgsqlDbType.Integer)
                            | :? int64 as x -> w.Write<int64>(x, NpgsqlDbType.Bigint)
                            | :? String as x -> w.Write<String>(x, NpgsqlDbType.Text)
                            | :? TimeSpan as x -> w.Write<TimeSpan>(x, NpgsqlDbType.Interval)
                            | :? array<byte> as x -> w.Write<byte[]>(x, NpgsqlDbType.Bytea)
                            | :? json as x -> w.Write(x.Data, NpgsqlDbType.Jsonb)
                            | x -> w.Write(x, NpgsqlDbType.Unknown))
                
                member p.RowAsync(o) = async { p.Row(o) }
                member p.FinishAsync() = async { p.Finish() } }

        member __.HasNestedTransactions = true
        member __.NestedTransaction(con, t) =
            let name = "savepoint" + savepoint.ToString()
            savepoint <- savepoint + 1
            match t.Connection with
            | null -> ()
            | _ -> t.Save(name)
            name
        member __.CommitNested(con, t, s) = savepoint <- savepoint - 1
        member __.RollbackNested(con, t, name) =
            savepoint <- savepoint - 1
            match t.Connection with
            | null -> ()
            | _ -> t.Rollback(name) }
