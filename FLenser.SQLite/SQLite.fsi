module FLenser.SQLite.Provider
open System
open System.Data.SQLite
open FLenser.Core

val create: SQLiteConnectionStringBuilder 
    -> IProvider<SQLiteConnection,SQLiteParameter,SQLiteTransaction>
