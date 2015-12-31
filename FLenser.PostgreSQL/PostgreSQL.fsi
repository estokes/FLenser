module FLenser.PostgreSQL.Provider
open System
open Npgsql
open FLenser.Core

val create: NpgsqlConnectionStringBuilder 
    -> IProvider<NpgsqlConnection,NpgsqlParameter,NpgsqlTransaction>
