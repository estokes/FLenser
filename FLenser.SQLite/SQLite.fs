module FLenser.SQLite.Provider
open System
open FSharpx
open System.Data.SQLite
open FLenser.Core

let create (cs: SQLiteConnectionStringBuilder) =
    let mutable savepoint = 0
    let prepInsert con table columns = 
        let pars = columns |> Array.map (fun (n: String) -> SQLiteParameter(n))
        let parnames = columns |> Array.map (fun n -> ":" + n)
        let sql = 
            sprintf "INSERT INTO %s (%s) VALUES (%s)"
                table (String.Join(", ", columns)) (String.Join(", ", parnames))
        let cmd = new SQLiteCommand(sql, con)
        cmd.Parameters.AddRange pars
        cmd.Prepare ()
        (pars, cmd)
    {new IProvider<SQLiteConnection, SQLiteParameter, SQLiteTransaction> with
         member __.Dispose() = ()
         member __.ConnectAsync() = async {
            let con = new SQLiteConnection(cs.ConnectionString)
            do! con.OpenAsync() |> Async.AwaitTask
            return con }

         member __.Connect() = 
            let con = new SQLiteConnection(cs.ConnectionString)
            con.Open ()
            con

         member __.CreateParameter(name) = SQLiteParameter(name)
         member __.InsertObjectAsync(con, table, columns) =
            let (pars, cmd) = prepInsert con table columns
            fun items -> async {
                do! items 
                    |> Seq.map (fun o ->
                        for i=0 to Array.length o - 1 do pars.[i].Value <- o.[i]
                        cmd.ExecuteNonQueryAsync() |> Async.AwaitTask)
                    |> Seq.toList
                    |> Async.sequence
                    |> Async.Ignore }

         member __.InsertObject(con, table, columns) =
            let (pars, cmd) = prepInsert con table columns
            fun items ->
                items |> Seq.iter (fun o ->
                    for i=0 to Array.length o - 1 do pars.[i].Value <- o.[i]
                    (cmd.ExecuteNonQuery() : int) |> ignore)

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
