module FLenser.PostgreSQL
open System
open Npgsql
open FLenser.Core

val provider: NpgsqlConnectionStringBuilder -> IProvider<_,_,_>
