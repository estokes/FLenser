(*  FLenser, a simple ORM for F#
    Copyright (C) 2015 Eric Stokes 

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Library General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>. *)
(* Why?

* There aren't any object relational mappers that are designed speficially for F# record and variant types.
Records, options, and variants are a nice way to think about data, and they map fairly well to database
types. 

* I prefer to write SQL directly, its a high level language all on it's own. Getting good performance
out of a relational database is still enough of an art that I don't want another layer of translation
in between me and SQL (e.g. Linq is cool, but I think ultimatly a burden).

* I don't mind manually keeping the database schema in sync with the types. In fact for complex
databases this makes life easier as the schema evolves. I don't think tight coupling between the 
database schema and the programming language types is a big win. *)

namespace FLenser.Core
open System
open System.Collections.Generic
open System.Data.Common

(* A virtual type field is a field in the database that is derived from
   a function on the F# type. A virtual type field will exist as a column
   in the database, but need not exist in the type at all. *)
type virtualTypeField<'A> = 'A -> obj

(* a virtual db field is an F# type field that is derived from a function
   on the columns of the database. The first argument is the nesting prefix of
   the type, the second is the fully qualified name of the field, should you need that. 
   By default nothing will be done with the field when the type is inserted into the database, 
   if you want to do something with it then also make a virtual type field. *)
type virtualDbField = String -> String -> DbDataReader -> obj

(* A Lens bidirectionally maps an F# algebraic data type to a database row type. Lenses only work on
   F# algebraic types (records, unions, and tuples), however if those types contain some other object
   type then it will be pickled to JSON using FsPickler (because many databases support operations on 
   JSON documents).

   Translation begins with primitive type mappings. Each provider has it's own type mappings, for 
   example here are the ones for PostgreSQL.

   .net     <==> PostgreSQL
   Boolean  <==>       bool
   Double   <==>     double
   String   <==>       text
   DateTime <==>  timestamp
   TimeSpan <==>   interval
   byte[]   <==>      bytea
   int      <==>        int
   int64    <==>        int

   F# type fields map to database column names, which are prefixed with the string in the 
   prefix argument, if specified. Database table names are not significant to the translation. 
   The database schema isn't altered (or read) in any way, the user must manually ensure that the 
   schema is compatible. All fields of the type must appear in the database,  however all columns 
   of the database need not appear in the type. Columns with no corresponding type field will be 
   ignored.  

   Examples (note $ is the default nesting separator, but you can change it)

   type t = {x: int; y: String}     <==> create table _ (x int, 
                                                         y text)

   type u = {name: String; data: t} <==> create table _ (name text,
                                                         data$x int,
                                                         data$y text)

   type kind = 
      | Simple of tag:String * t 
      | Complex of u                <==> create table _ (kind int,
                                                         kind$Simple$tag text,
                                                         kind$Simple$Item2$x int, 
                                                         kind$Simple$Item2$y text, 
                                                         kind$Complex$Item$name text,
                                                         kind$Complex$Item$data$x int,
                                                         kind$Complex$Item$data$y text)

   type v = {id: int; it: kind}     <==> create table _ (id int, 
                                                         it int, 
                                                         it$Simple$tag text,
                                                         it$Simple$Item2$x int, 
                                                         it$Simple$Item2$y text, 
                                                         it$Complex$Item$name text,
                                                         it$Complex$Item$data$x int,
                                                         it$Complex$Item$data$y text)
    
    type tuple = v * t              <==> create table _ (Item1$id int,
                                                         Item1$it int, 
                                                         Item1$it$Simple$tag text,
                                                         Item1$it$Simple$Item2$x int, 
                                                         Item1$it$Simple$Item2$y text, 
                                                         Item1$it$Complex$Item$name text,
                                                         Item1$it$Complex$Item$data$x int,
                                                         Item1$it$Complex$Item$data$y text,
                                                         Item2$x int,
                                                         Item2$y text)
  
   The None case of the option type is encoded as SQL null when the Some case
   is a primitive type. Otherwise it is treated like any other union type. *)
[<Class>]
type lens<'A> =
    member Guid : Guid with get
    member Columns: String[] with get
    // Same as columns but not joined by the nesting sep
    member Paths: list<String>[] with get
    member Types: Type[] with get

type NonQuery

(* add this attribute to a static member of your
   records of type

   ?prefix:String -> lens<'A>

   And Lens.create will call this method and use
   the returned lens instead of processing the sub
   record. You can use this functionality to define
   virtual record fields and virtual db fields in just
   one place such that they still work correctly when the
   type is used as a sub record (e.g. say you're assembling
   a large join and reading the results into a toplevel record
   instead of a tuple) *)
[<Class>]
type CreateSubLensAttribute =
    inherit Attribute
    new: unit -> CreateSubLensAttribute

(* Flatten may be applied to non primitive record fields, and union cases. It causes the
   prefix to be replaced with the optional prefix argument (default is nothing). It's effect
   is to flatten the namespace in a nested type, hoisting children of the field up to the same
   level as the field itself. Flatten and rename may not be used together. *)
[<Class>]
type FlattenAttribute =
    inherit Attribute
    new: unit -> FlattenAttribute
    member Prefix: String with get, set

(* Rename may be applied to record fields and union cases. it causes the database column to
   be called something different than the field/case name. *)
[<Class>]
type RenameAttribute =
    inherit Attribute
    new: columnName:String -> RenameAttribute

[<Class>]
type Lens =
    // Create a lens for type A
    static member Create : ?virtualDbFields:Map<list<String>,virtualDbField> 
        * ?virtualTypeFields:Map<list<String>,virtualTypeField<'A>> 
        * ?prefix:list<String> * ?nestingSeparator:String -> lens<'A>
    // A special lens that reads nothing, used for side effecting queries
    static member NonQuery: lens<NonQuery> with get
    // Given an A Lens, produce an optional A Lens. When reading, if all columns of
    // A are null, the optional Lens produces None, otherwise it invokes the A Lens
    // and produces the result
    static member Optional: lens:lens<'A> -> lens<Option<'A>>
    // Given an A lens and a B lens produce a lens that reads/writes both
    // A and B and returns a tuple A * B. Useful for reading the results of
    // a join. If A and B have intersecting column names with the same data, 
    // for example the join field, you may add the intersecting column name
    // to the allowedIntersection set. Any intersecting column names not in
    // allowedIntersection will cause a runtime error to be raised when the
    // lens is built (normally during type initialization).
    static member Tuple: lensA:lens<'A> * lensB:lens<'B> 
        * ?allowedIntersection:Set<String> -> lens<'A * 'B>
    static member Tuple: lensA:lens<'A> * lensB:lens<'B> * lensC:lens<'C> 
        * ?allowedIntersection:Set<String> -> lens<'A * 'B * 'C>
    static member Tuple: lensA:lens<'A> * lensB:lens<'B> * lensC:lens<'C> 
        * lensD:lens<'D> * ?allowedIntersection:Set<String> 
        -> lens<'A * 'B * 'C * 'D>
    // The maximum tuple size is 5. However it's quite easy to add more
    // It just hasn't been necessary.
    static member Tuple: lensA:lens<'A> * lensB:lens<'B> * lensC:lens<'C> 
        * lensD:lens<'D> * lensE:lens<'E> * ?allowedIntersection:Set<String> 
        -> lens<'A * 'B * 'C * 'D * 'E>

type parameter<'A>

[<Class>]
type Parameter =
    static member String: name:String -> parameter<String>
    static member Int: name:String -> parameter<int>
    static member Int64: name:String -> parameter<int64>
    static member Float: name:String -> parameter<float>
    static member Bool: name:String -> parameter<bool>
    static member DateTime: name:String -> parameter<DateTime>
    static member TimeSpan: name:String -> parameter<TimeSpan>
    // This can't be used on OfLens paramaters
    static member Array: parameter<'A> -> parameter<'A[]>
    static member OfLens: lens<'A> * ?paramNestingSep:String -> parameter<'A>

[<Class>]
type query<'A, 'B> = 
    interface IDisposable
    member Guid: Guid with get
    member Sql: String with get
    member Parameters: (String * Type)[] with get

[<Class>]
type Query =
    static member Create: sql:String * lens:lens<'B>
        -> query<unit, 'B>
    static member Create: sql:String * lens:lens<'B> * p1:parameter<'P1>
        -> query<'P1, 'B>
    static member Create: sql:String * lens:lens<'B> 
        * p1:parameter<'P1> * p2:parameter<'P2>
        -> query<'P1 * 'P2, 'B>
    static member Create: sql:String * lens:lens<'B> 
        * p1:parameter<'P1> * P2:parameter<'P2> * p3:parameter<'P3>
        -> query<'P1 * 'P2 * 'P3, 'B>
    static member Create: sql:String * lens:lens<'B>
        * p1:parameter<'P1> * p2:parameter<'P2> * p3:parameter<'P3> * p4:parameter<'P4>
        -> query<'P1 * 'P2 * 'P3 * 'P4, 'B>
    static member Create: sql:String * lens:lens<'B>
        * p1:parameter<'P1> * p2:parameter<'P2> * p3:parameter<'P3> 
        * p4:parameter<'P4> * p5:parameter<'P5>
        -> query<'P1 * 'P2 * 'P3 * 'P4 * 'P5, 'B>
    static member Create: sql:String * lens:lens<'B>
        * p1:parameter<'P1> * p2:parameter<'P2> * p3:parameter<'P3> 
        * p4:parameter<'P4> * p5:parameter<'P5> * p6:parameter<'P6>
        -> query<'P1 * 'P2 * 'P3 * 'P4 * 'P5 * 'P6, 'B>
    static member Create: sql:String * lens:lens<'B>
        * p1:parameter<'P1> * p2:parameter<'P2> * p3:parameter<'P3> 
        * p4:parameter<'P4> * p5:parameter<'P5> * p6:parameter<'P6>
        * p7:parameter<'P7>
        -> query<'P1 * 'P2 * 'P3 * 'P4 * 'P5 * 'P6 * 'P7, 'B>
    static member Create: sql:String * lens:lens<'B>
        * p1:parameter<'P1> * p2:parameter<'P2> * p3:parameter<'P3> 
        * p4:parameter<'P4> * p5:parameter<'P5> * p6:parameter<'P6>
        * p7:parameter<'P7> * p8:parameter<'P8>
        -> query<'P1 * 'P2 * 'P3 * 'P4 * 'P5 * 'P6 * 'P7 * 'P8, 'B>
    static member Create: sql:String * lens:lens<'B>
        * p1:parameter<'P1> * p2:parameter<'P2> * p3:parameter<'P3> 
        * p4:parameter<'P4> * p5:parameter<'P5> * p6:parameter<'P6>
        * p7:parameter<'P7> * p8:parameter<'P8> * p9:parameter<'P9>
        -> query<'P1 * 'P2 * 'P3 * 'P4 * 'P5 * 'P6 * 'P7 * 'P8 * 'P9, 'B>
    static member Create: sql:String * lens:lens<'B>
        * p1:parameter<'P1> * p2:parameter<'P2> * p3:parameter<'P3> 
        * p4:parameter<'P4> * p5:parameter<'P5> * p6:parameter<'P6>
        * p7:parameter<'P7> * p8:parameter<'P8> * p9:parameter<'P9>
        * p10:parameter<'P10>
        -> query<'P1 * 'P2 * 'P3 * 'P4 * 'P5 * 'P6 * 'P7 * 'P8 * 'P9 * 'P10, 'B>

type PreparedInsert =
    inherit IDisposable
    abstract member Finish: unit -> unit
    abstract member Row: obj[] -> unit
    abstract member FinishAsync: unit -> Async<unit>
    abstract member RowAsync: obj[] -> Async<unit>

// When a lens chooses to serialize something to Json, it will pass
// this type to the database provider instead of the raw string
// which is returned by Data. Some databases need to be told
[<Class>]
type json =
    member Data: String with get

// A provider is required to interface with an APO.NET implementation,
// and should be easy to derive from such an implementation
type Provider<'CON, 'PAR, 'TXN
               when 'CON :> DbConnection
                and 'CON : not struct
                and 'PAR :> DbParameter
                and 'TXN :> DbTransaction> =
    inherit IDisposable
    abstract member ConnectAsync: unit -> Async<'CON>
    abstract member Connect: unit -> 'CON
    abstract member CreateParameter: name:String * typ:Type -> 'PAR
    abstract member PrepareInsert: 'CON * table:String * columns:String[] 
        -> PreparedInsert
    abstract member HasNestedTransactions: bool
    abstract member BeginTransaction: 'CON -> 'TXN
    abstract member NestedTransaction: 'CON * 'TXN -> String
    abstract member RollbackNested: 'CON * 'TXN * String -> unit
    abstract member CommitNested: 'CON * 'TXN * String -> unit

module Async =
    type db =
        inherit IDisposable

        // Execute a query, returning it's result
        abstract member Query: query<'A, 'B> * 'A -> Async<List<'B>>

        // Execute a mutator, returning the number of rows altered
        abstract member NonQuery: query<'A, NonQuery> * 'A -> Async<int>

        // insert the sequence of objects into the named table using the 
        // specified lens. Most providers implement this with a special db
        // specific bulk insertion mechanism. For example COPY in Postgresql.
        abstract member Insert: table:String * lens<'A> * seq<'A> -> Async<unit>

        // Execute a set of Query and NonQuery calls as an atomic transaction.
        // In the transaction you must use ONLY the db object passed
        // in to you. You must not keep a reference to this object that
        // lives longer than the scope of the function.
        abstract member Transaction: (db -> Async<'a>) -> Async<'a>

        // If this db would normally retry on error, turn that off
        // for every operation done inside the closure.
        // Within the closure you much used the passed in db object, and
        // you must not save that object anywhere.
        abstract member NoRetry: (db -> Async<'A>) -> Async<'A>

    [<Class>]
    type Db =
        static member Connect: Provider<_,_,_> -> Async<db>

        // This will return a db that will retry queries marked safetoretry
        // on error. This may in rare cases result in multiple successful executions
        // of a query, so ensure that queries marked safe to retry are idempotent
        static member WithRetries: Provider<_,_,_>
            * ?log:(Exception -> unit) 
            * ?tries:int 
            -> Async<db>

type db =
    inherit IDisposable
    abstract member Query: query<'A, 'B> * 'A -> List<'B>
    abstract member NonQuery: query<'A, NonQuery> * 'A -> int
    abstract member Insert: table:String * lens<'A> * seq<'A> -> unit
    abstract member Transaction: (db -> 'a) -> 'a

[<Class>]
type Db =
    // Since most db operations take a long time this should be used
    // with caution. However if you have an in memory db it can reduce
    // overhead significantly. Most users will want ConnectAsync.
    static member Connect: Provider<_,_,_> -> db
