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
module FLenser.Mono.SQLite.Provider
open System
open FSharpx
open FSharpx.Control
open System.Data.SQLite
open FLenser.Core

let create (cs: SQLiteConnectionStringBuilder) =
    let mutable savepoint = 0
    {new Provider<SQLiteConnection, SQLiteParameter, SQLiteTransaction> with
         member __.Dispose() = ()
         member __.ConnectAsync() = async {
            let con = new SQLiteConnection(cs.ConnectionString)
            do! con.OpenAsync() |> Async.AwaitTask
            return con }

         member __.Connect() = 
            let con = new SQLiteConnection(cs.ConnectionString)
            con.Open ()
            con

         member __.CreateParameter(name, _) = SQLiteParameter(name)
         member __.PrepareInsert(con, table, columns) =
            let pars = columns |> Array.map (fun (n: String) -> SQLiteParameter(n))
            let parnames = columns |> Array.map (fun n -> ":" + n)
            let sql = 
                sprintf "INSERT INTO %s (%s) VALUES (%s)"
                    table (String.Join(", ", columns)) (String.Join(", ", parnames))
            let cmd = new SQLiteCommand(sql, con)
            cmd.Parameters.AddRange pars
            cmd.Prepare ()
            let set (o : obj[]) = for i=0 to o.Length - 1 do pars.[i].Value <- o.[i]
            {new PreparedInsert with
                member __.Dispose() = cmd.Dispose()
                member __.Row(o) =
                    set o
                    ignore (cmd.ExecuteNonQuery() : int)
                member __.Finish() = ()
                member __.RowAsync(o) =
                    set o
                    cmd.ExecuteNonQueryAsync()
                    |> Async.AwaitTask
                    |> Async.Ignore
                member __.FinishAsync() = Async.unit }
         member __.HasNestedTransactions = true
         member __.BeginTransaction(con) = con.BeginTransaction()
         member __.NestedTransaction(con, txn) =
            let name = "savepoint" + savepoint.ToString()
            savepoint <- savepoint + 1
            use cmd = con.CreateCommand()
            cmd.CommandText <- "savepoint " + name
            ignore (cmd.ExecuteNonQuery() : int)
            name
         member __.CommitNested(con, txn, name) =
            savepoint <- savepoint - 1
            use cmd = con.CreateCommand()
            cmd.CommandText <- "RELEASE " + name
            ignore (cmd.ExecuteNonQuery() : int)
         member __.RollbackNested(con, txn, name) =
            savepoint <- savepoint - 1
            use cmd = con.CreateCommand()
            cmd.CommandText <- "ROLLBACK TO SAVEPOINT " + name
            ignore (cmd.ExecuteNonQuery () : int) }
