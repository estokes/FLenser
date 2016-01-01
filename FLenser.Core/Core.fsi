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

// A virtual record field is a field in the database that is derived from
// a function on the underlying record, it need not exist in the record.
// This is useful for generating indices
type virtualRecordField<'A> = DbParameter -> 'A -> unit

(* a virtual db field is a field that exists in the record but 
   is derived from other fields in the database. It is possible to
   specify virtual db fields in nested records by prefixing the
   name of the field containing the nested record to the name of the
   field in the nested recorf. E.G.

   type a =
      { foo: String
        bar: int
        virtualfld: float }
   
   type b = 
      { baz: String
        zam: DateTime
        fld: a }

   then the name of the virtual field in a, in a lens processing b is
   "fld$virtualfld"
*) 
type virtualDbField = String -> DbDataReader -> obj

(* A Lens bidirectionally maps a F# record type to a database row type. Lenses only work on
   F# record types, however those record types may contain other records, variants, arrays, and
   other types (more complex ones via json serialization)
    
   Translation begins with the following primitive mappings
   .net     <==>        Db
   Boolean  <==>      bool
   Double   <==>    double
   String   <==>      text
   DateTime <==> timestamp
   TimeSpan <==>  interval
   byte[]   <==>     bytea
   int      <==>       int
   int64    <==>       int

   F# record fields map to database column names, which are prefixed with the string in the 
   prefix argument, if specified. Database table names are not significant to the translation,
   which is purely structural. The database schema isn't altered in any way, the user must manually
   ensure that the schema matches, else the translation will fail at run time. All fields of the record
   must appear in the database, however all columns of the database need not appear in the record. Columns
   with no corresponding record field will be ignored. This property is intentional, and very useful for
   extending the database schema without disturbing existing applications. 

   Examples

   type t = {x: int; y: String} <==> create table _ (x int, y text)
   type u = {name: String; data: t} <==> create table _ (name text, data$x int, data$y text)
   type kind = Simple of t | Complex of u
   type v = {id: int; it: kind} <==> create table _ (id int, it int, it$complex$name text, 
                                                     it$complex$data$x int, it$complex$data$y text, 
                                                     it$simple$x int, it$simple$y int)
  
   The option type is handled specially using SQL null
   type x = {foo: Option<String>} <==> create table _ (foo text)
   if column foo is null for a specific row then {foo = None} will be
   generated. If {foo = None} is inserted, then column foo will be set
   to null.
  
   Sub records can be optional, Variants may not
   type y = {foo: Option<t>} <==> create table _ (foo$x int, foo$y text)
   If all columns of an optional sub record are null, then it is None,
   otherwise it is Some
  
   Depending on the database provider Arrays of primitives may be represented as database arrays
   type a = {name: String; items:String[]} <==> create table _ (name text, items text[])

   Any type that isn't a primitive, subrecord, optional subrecord, variant
   with only nothing, or records as args, or an array will be serialized to JSONB. E.G.
   type b = {name: String; items: list<String * String>} <==> create table _ (name text, items jsonb)
*)
[<Class>]
type lens<'A> =
    member Guid : Guid with get
    member ColumnNames : string [] with get

type NonQuery

// add this attribute to a static member of your
// records of type
//
// ?prefix:String -> lens<'A>
//
// And Lens.create will call this method and use
// the returned lens instead of processing the sub
// record. You can use this functionality to define
// virtual record fields and virtual db fields in just
// one place such that they still work correctly when the
// type is used as a sub record (e.g. say you're assembling
// a large join and reading the results into a toplevel record
// instead of a tuple)
[<Class>]
type CreateSubLensAttribute =
    inherit Attribute
    new: unit -> CreateSubLensAttribute

[<Class>]
type Lens =
    // Create a lens for type A
    static member Create : ?virtualDbFields:Map<String,virtualDbField> 
        * ?virtualRecordFields:Map<String,virtualRecordField<'A>> 
        * ?prefix:string -> lens<'A>
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

[<Class>]
type parameter<'A> = 
    member Name: String

[<Class>]
type Parameter =
    static member Create: name:String -> parameter<'A>
    static member String: name:String -> parameter<String>
    static member Int: name:String -> parameter<int>
    static member Int64: name:String -> parameter<int64>
    static member Float: name:String -> parameter<float>
    static member Bool: name:String -> parameter<bool>
    static member ByteArray: name:String -> parameter<byte[]>
    static member DateTime: name:String -> parameter<DateTime>
    static member TimeSpan: name:String -> parameter<TimeSpan>

[<Class>]
type query<'A, 'B> = 
    interface IDisposable
    member Guid: Guid with get
    member Sql: String with get
    member Parameters: String[] with get

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

// A provider is required to interface with an APO.NET implementation,
// and should be easy to derive from such an implementation
type IProvider<'CON, 'PAR, 'TXN
                when 'CON :> DbConnection
                 and 'CON : not struct
                 and 'PAR :> DbParameter
                 and 'TXN :> DbTransaction> =
    inherit IDisposable
    abstract member ConnectAsync: unit -> Async<'CON>
    abstract member Connect: unit -> 'CON
    abstract member CreateParameter: name:String -> 'PAR
    abstract member InsertObjectAsync: 'CON * table:String * columns:String[]
        -> (seq<obj[]> -> Async<unit>)
    abstract member InsertObject: 'CON * table:String * columns:String[]
        -> (seq<obj[]> -> unit)
    abstract member HasNestedTransactions: bool
    abstract member BeginTransaction: 'CON -> 'TXN
    abstract member NestedTransaction: 'CON * 'TXN -> String
    abstract member RollbackNested: 'CON * 'TXN * String -> unit
    abstract member CommitNested: 'CON * 'TXN * String -> unit

type asyncdb =
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
    abstract member Transaction: (asyncdb -> Async<'a>) -> Async<'a>

    // If this db would normally retry on error, turn that off
    // for every operation done inside the closure.
    // Within the closure you much used the passed in db object, and
    // you must not save that object anywhere.
    abstract member NoRetry: (asyncdb -> Async<'A>) -> Async<'A>

type db =
    inherit IDisposable
    abstract member Query: query<'A, 'B> * 'A -> List<'B>
    abstract member NonQuery: query<'A, NonQuery> * 'A -> int
    abstract member Insert: table:String * lens<'A> * seq<'A> -> unit
    abstract member Transaction: (db -> 'a) -> 'a
    abstract member NoRetry: (db -> 'A) -> 'A

[<Class>]
type Db =
    // Since most db operations take a long time this should be used
    // with caution. However if you have an in memory db it can reduce
    // overhead significantly. Most users will want ConnectAsync.
    static member Connect: IProvider<_,_,_> -> db

    static member ConnectAsync: IProvider<_,_,_> -> Async<asyncdb>

    // This will return a db that will retry queries marked safetoretry
    // on error. This may in rare cases result in multiple successful executions
    // of a query, so ensure that queries marked safe to retry are idempotent
    static member WithRetries: IProvider<_,_,_>
        * ?log:(Exception -> unit) 
        * ?tries:int 
        -> Async<asyncdb>
