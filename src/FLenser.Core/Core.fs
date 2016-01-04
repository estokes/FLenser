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
namespace FLenser.Core

open System
open System.Collections.Generic
open System.Data.Common
open FSharpx
open FSharpx.Control
open Nessos.FsPickler
open Nessos.FsPickler.Json
open Microsoft.FSharp.Reflection
open System.Reflection

exception UnexpectedNull

type virtualTypeField<'A> = 'A -> obj
type virtualDbField = String -> DbDataReader -> obj

type internal rawLens = 
    abstract member Columns: String[] with get
    abstract member InjectRaw: obj[] * int * obj -> unit
    abstract member ProjectRaw: DbDataReader -> obj

type lens<'A> internal (columns: String[], 
                        inject: obj[] -> int -> 'A -> unit, 
                        project: DbDataReader -> 'A) =
    let guid = Guid.NewGuid ()
    member __.Guid with get() = guid
    member __.Columns with get() = columns
    member internal __.Inject(target: obj[], startidx:int, v) = inject target startidx v
    member internal __.Project(r) = project r
    interface rawLens with
        member __.Columns with get() = columns
        member o.InjectRaw(target: obj[], startidx: int, v : obj) =
           o.Inject(target, startidx, v :?> 'A)
        member o.ProjectRaw(r) = box (project r)

type NonQuery = {nothing: unit}

module Utils =
    type Sequencer() =
        let mutable running = false
        let jobs = Queue<Async<Unit> * AsyncResultCell<Unit>>()
        let rec loop () = async {
            let job =
                lock jobs (fun () -> 
                    if jobs.Count > 0 then Some (jobs.Dequeue ())
                    else 
                        running <- false
                        None)
            match job with
            | None -> ()
            | Some (job, finished) ->
                Async.Start job
                do! finished.AsyncResult
                return! loop () }
        member __.Enqueue(job : Async<'A>) : Async<'A> =
            let res = AsyncResultCell()
            let finished = AsyncResultCell()
            let job = async {
                try
                    let! z = job
                    res.RegisterResult (AsyncOk z)
                with e -> 
                    return res.RegisterResult (AsyncException e)
                finished.RegisterResult (AsyncOk ()) }
            lock jobs (fun () ->
                jobs.Enqueue(job, finished)
                if running = false then Async.StartImmediate (loop ()))
            res.AsyncResult

    type Result<'A, 'B> =
        | Ok of 'A
        | Error of 'B

    let ord (r: DbDataReader) name = 
        try r.GetOrdinal name
        with e -> failwith (sprintf "could not find field %s %A" name e)

    let tuple (lensA: lens<'T1>) (lensB: lens<'T2>) allowedIntersection:lens<'T1 * 'T2> =
        let c (l : lens<_>) = l.Columns |> Set.ofArray
        let is = Set.difference (Set.intersect (c lensA) (c lensB)) allowedIntersection
        if not (Set.isEmpty is) then
            failwith (sprintf "column names have a non empty intersection %A" is)
        let columns = Array.append lensA.Columns lensB.Columns
        lens<'T1 * 'T2>(columns, 
            (fun cols startidx (v1, v2) -> 
                lensA.Inject(cols, startidx, v1)
                lensB.Inject(cols, startidx + lensA.Columns.Length, v2)),
            (fun r -> (lensA.Project r, lensB.Project r)))

    let ofInjProj (merged: lens<_>) inject project = 
        lens<_>(merged.Columns, inject, project)

    let ai a = defaultArg a Set.empty

    let NonQuery = {nothing = ()}
    let NonQueryLens =
        let guid = Guid.NewGuid()
        let project (r: DbDataReader) = ignore r
        lens<NonQuery>([||], (fun _ _ _ -> ()), (fun _ -> NonQuery))

    let getp (r: DbDataReader) name f = f (ord r name)
    let objToInt32 (o: obj) =
        match o with
        | :? int32 as x -> x
        | :? byte as x -> int32 x
        | :? sbyte as x -> int32 x
        | :? uint16 as x -> int32 x
        | :? int16 as x -> int32 x
        | :? uint32 as x -> int32 x
        | :? uint64 as x -> int32 x
        | :? int64 as x -> int32 x
        | o -> failwith (sprintf "cannot convert %A to int32" o)
    let primitives = 
        let std r n = getp r n r.GetValue
        [ typeof<bool>, 
            (fun r n -> getp r n (fun i ->
                match r.GetValue i with
                | :? bool as x -> x
                | :? byte as x -> x > 0uy
                | :? int16 as x -> x > 0s
                | :? int32 as x -> x > 0
                | :? int64 as x -> x > 0L
                | o -> failwith (sprintf "can't cast %A to boolean" o)
                |> box))
          typeof<float>, std
          typeof<String>, std
          typeof<array<byte>>, std
          typeof<array<int16>>, std
          typeof<array<int32>>, std
          typeof<array<int64>>, std
          typeof<array<float>>, std
          typeof<array<bool>>, std
          typeof<array<String>>, std
          typeof<array<DateTime>>, std
          typeof<array<TimeSpan>>, std
          typeof<DateTime>, 
            (fun r n -> getp r n (fun i ->
                match r.GetValue i with
                | :? DateTime as x -> x
                | :? String as x -> DateTime.Parse x
                | o -> failwith (sprintf "can't cast %A to DateTime" o)
                |> box))
          typeof<TimeSpan>, 
            (fun r n -> getp r n (fun i ->
                match r.GetValue i with
                | :? TimeSpan as x -> x
                | :? String as x -> TimeSpan.Parse x
                | o -> failwith (sprintf "can't cast %A to TimeSpan" o)
                |> box))
          typeof<byte>,
            (fun r n -> getp r n (fun i ->
                match r.GetValue i with
                | :? byte as x -> x
                | :? int16 as x -> byte x
                | :? int32 as x -> byte x
                | :? int64 as x -> byte x
                | o -> failwith (sprintf "cannot convert %A to byte" o)
                |> box))
          typeof<int16>,
            (fun r n -> getp r n (fun i ->
                match r.GetValue i with
                | :? int16 as x -> x
                | :? byte as x -> int16 x
                | :? int32 as x -> int16 x
                | :? int64 as x -> int16 x
                | o -> failwith (sprintf "cannot convert %A to int16" o)
                |> box))
          typeof<int>, 
            (fun r n -> getp r n (fun i ->
                box (objToInt32 (r.GetValue i))))
          typeof<int64>, 
            (fun r n -> getp r n (fun i ->
                match r.GetValue i with
                | :? int64 as x -> x
                | :? byte as x -> int64 x
                | :? int16 as x -> int64 x
                | :? int32 as x -> int64 x
                | o -> failwith (sprintf "cannot convert %A to int64" o)
                |> box)) ]
        |> dict

    let isPrim (typ: Type) = primitives.ContainsKey typ
    let isPrimOption (typ: Type) =
        typ.IsGenericType
        && typ.GetGenericTypeDefinition() = typedefof<Option<_>> 
        && (isPrim typ.GenericTypeArguments.[0])
    let isRecord t = FSharpType.IsRecord(t, true)
    let isUnion t = FSharpType.IsUnion(t, true)
    let isTuple t = FSharpType.IsTuple(t)
    let isRecursive (t: Type) =
        let rec loop t' =
            if isPrim t' || isPrimOption t' then false
            elif isRecord t' then
                FSharpType.GetRecordFields(t', true)
                |> Array.exists (fun fld ->
                    fld.PropertyType = t
                    || if fld.PropertyType = t' then false
                       else loop fld.PropertyType)
            elif isUnion t' then
                FSharpType.GetUnionCases(t', true)
                |> Array.exists (fun c ->
                    c.GetFields()
                    |> Array.exists (fun fld ->
                        fld.PropertyType = t
                        || if fld.PropertyType = t' then false
                           else loop fld.PropertyType))
            elif isTuple t' then
                FSharpType.GetTupleElements(t')
                |> Array.exists (fun ptyp -> 
                    ptyp = t || if ptyp = t' then false else loop ptyp)
            else false
        loop t

open Utils

type CreateSubLensAttribute() = inherit Attribute()

type Lens =
    static member NonQuery with get() = NonQueryLens
    static member Create<'A>(?virtualDbFields: Map<String, virtualDbField>,
                             ?virtualTypeFields: Map<String, virtualTypeField<'A>>,
                             ?prefix, ?nestingSeparator) : lens<'A> =
        let prefix = defaultArg prefix ""
        let sep = defaultArg nestingSeparator "$"
        let virtualDbFields = 
            defaultArg virtualDbFields Map.empty
            |> Map.fold (fun m k v -> Map.add (prefix + k) v m) Map.empty
        let virtualTypeFields = defaultArg virtualTypeFields Map.empty
        let typ = typeof<'A>
        let jsonSer = FsPickler.CreateJsonSerializer(indent = false, omitHeader = true)
        let columns = List<_>(capacity = 10)
        let createColumnSlot name =
            let id = columns.Count
            columns.Add name
            id
        let vdf = 
            virtualTypeFields 
            |> Map.map (fun k v -> createColumnSlot (prefix + k), v)
        let prim (inject: obj -> obj) 
            (project: DbDataReader -> String -> obj)
            (name: String) (nullok: bool) =
            let colid = createColumnSlot name
            let inject (a: obj[]) startidx (record : obj) : unit = 
                a.[colid + startidx] <- inject record
            let project (r: DbDataReader) : obj = 
                let o = project r name
                match o with
                | null when not nullok -> raise UnexpectedNull
                | o -> o
            (inject, project)
        let primOpt (reader : obj -> obj)
            (project : DbDataReader -> String -> obj) (typ: Type)
            (name: String) =
            let ucases = FSharpType.GetUnionCases(typ)
            let none = ucases |> Array.find (fun c -> c.Name = "None")
            let some = ucases |> Array.find (fun c -> c.Name = "Some")
            let get = FSharpValue.PreComputeUnionReader(some)
            let get (o : obj) = (get o).[0]
            let tag = FSharpValue.PreComputeUnionTagReader(typ)
            let mkNone = FSharpValue.PreComputeUnionConstructor none
            let mkSome = FSharpValue.PreComputeUnionConstructor some
            let rtyp = typ.GenericTypeArguments.[0]
            let inject v =
               let o = reader v
               let tag = tag o
               if tag = none.Tag then null
               else if tag = some.Tag then get o
               else failwith "bug"
            let project (r: DbDataReader) name : obj =
                if r.IsDBNull(ord r name) then mkNone [||]
                else 
                    let o = project r name
                    mkSome [|o|]
            prim inject project name true
        let json (typ: Type) reader name =
            let pk = FsPickler.GeneratePickler(typ)
            let e = System.Text.Encoding.UTF8
            prim 
                (fun v -> box (e.GetString (jsonSer.PickleUntyped(reader v, pk))))
                (fun r n -> 
                    let s = 
                        match r.GetValue(ord r n) with
                        | :? array<byte> as x -> x
                        | :? String as x -> e.GetBytes x
                        | o -> failwith (sprintf "can't convert %A to byte[]" o)
                    jsonSer.UnPickleUntyped(s, pk, encoding = e))
                name false
        let readFldFromPrecomputed (r: obj -> obj[]) =
            let mutable the = obj()
            let mutable cached = [||]
            (fun (o: obj) i -> 
                if LanguagePrimitives.PhysicalEquality o the then cached.[i]
                else
                    the <- o
                    cached <- r o
                    cached.[i]),
            (fun () -> the <- obj(); cached <- [||])
        let rec subLens (typ: Type) reader prefix =
            typ.GetMethods() 
            |> Seq.filter (fun mi -> 
                mi.CustomAttributes 
                |> Seq.exists (fun a -> 
                    a.AttributeType = typeof<CreateSubLensAttribute>)) 
            |> Seq.toList
            |> function
                | [] -> createInjectProject typ reader prefix
                | [mi] ->
                    let pars = mi.GetParameters()
                    let ret = mi.ReturnParameter.ParameterType
                    if ret.IsGenericType
                       && ret.GetGenericTypeDefinition() = typedefof<lens<_>>
                       && mi.IsStatic
                       && ret.GenericTypeArguments.[0] = typ
                       && Array.length pars = 1
                       && (let (p, ptyp) = pars.[0], pars.[0].ParameterType
                           p.Name = "prefix"
                           && ptyp.IsGenericType
                           && ptyp.GetGenericTypeDefinition() = typedefof<Option<_>>
                           && ptyp.GenericTypeArguments.[0] = typeof<String>)
                    then 
                        let lens = mi.Invoke(null, [|(Some prefix) :> obj|]) :?> rawLens
                        let startidx = columns.Count
                        columns.AddRange lens.Columns
                        ((fun a startidx' o -> lens.InjectRaw(a, startidx + startidx', reader o)), 
                         lens.ProjectRaw)
                    else failwith 
                            (sprintf "CreateSubLens marked method does not 
                                      match the required signature
                                      static member _: ?prefix:String -> lens<'A>
                                      %A" mi)
                | _ -> failwith (sprintf "multiple CreateSubLens in %A" typ)
        and record (typ: Type) reader prefix =
            let fields = FSharpType.GetRecordFields(typ, true)
            let injectAndProject = fields |> Array.map (fun fld ->
                let readFld = FSharpValue.PreComputeRecordFieldReader fld
                let reader o = readFld (reader o)
                let name = prefix + fld.Name
                let typ = fld.PropertyType
                match Map.tryFind name virtualDbFields with
                | Some project -> (fun _ _ _ -> ()), project prefix
                | None ->
                    if isPrim typ then
                        prim reader primitives.[typ] name false
                    elif isPrimOption typ then
                        primOpt reader primitives.[typ.GenericTypeArguments.[0]]
                            fld.PropertyType name
                    elif not (isRecursive typ) && (isRecord typ || isTuple typ) then 
                        subLens typ reader (name + sep)
                    elif not (isRecursive typ) && isUnion typ then 
                        subLens typ reader name
                    else json typ reader name)
            let construct = FSharpValue.PreComputeRecordConstructor(typ, true)
            let inject cols startidx record = 
                injectAndProject |> Array.iter (fun (inj, _) -> inj cols startidx record)
            let project r =
                let a = injectAndProject |> Array.map (fun (_, proj) -> proj r)
                construct a
            (inject, project)
        and union (typ: Type) reader prefix =
            let prefix = if prefix = "" then typ.Name else prefix
            let colid = createColumnSlot prefix
            let tag = FSharpValue.PreComputeUnionTagReader(typ, true)
            let mutable clearPrecomputedCache = []
            let case (c: UnionCaseInfo) =
                let fields = c.GetFields ()
                let injectAndProject =
                    if fields = [||] then [||]
                    else
                        let (readFld, clearCache) = 
                            readFldFromPrecomputed 
                                (FSharpValue.PreComputeUnionReader(c, true))
                        clearPrecomputedCache <- clearCache :: clearPrecomputedCache
                        fields |> Array.mapi (fun i fld ->
                            let reader o = readFld (reader o) i
                            let typ = fld.PropertyType
                            let name = prefix + sep + c.Name + sep + fld.Name
                            match Map.tryFind name virtualDbFields with
                            | Some project -> (fun _ _ _ -> ()), project prefix
                            | None ->
                                if isPrim typ then
                                    prim reader primitives.[typ] name false
                                elif isPrimOption typ then
                                    primOpt reader 
                                        primitives.[typ.GenericTypeArguments.[0]]
                                        fld.PropertyType name
                                elif not (isRecursive typ) 
                                     && (isRecord typ || isTuple typ) then
                                    subLens typ reader (name + sep)
                                elif not (isRecursive typ) && isUnion typ then 
                                    subLens typ reader name
                                else json typ reader name)
                let inject cols sidx case =
                    injectAndProject |> Array.iter (fun (inj, _) -> inj cols sidx case)
                let project c = injectAndProject |> Array.map (fun (_, proj) -> proj c)
                (inject, project)
            let mk =
                FSharpType.GetUnionCases(typ) 
                |> Array.map (fun c -> 
                    c.Tag, (FSharpValue.PreComputeUnionConstructor(c), case c))
                |> dict
            let inject (cols: obj[]) (startidx: int) (v: obj) =
                let tag = tag (reader v)
                cols.[startidx + colid] <- box tag
                let (_mk, (inj, _)) = mk.[tag]
                inj cols startidx v
                clearPrecomputedCache |> List.iter (fun f -> f ())
            let project (r: DbDataReader) =
                let i = objToInt32 (r.GetValue(ord r prefix))
                let (mk, (_, proj)) = mk.[i]
                mk (proj r)
            (inject, project)
        and tuple (typ: Type) reader prefix =
            let flds = FSharpType.GetTupleElements typ
            let (readFld, clearCache) = 
                readFldFromPrecomputed (FSharpValue.PreComputeTupleReader typ)
            let injectProject = flds |> Array.mapi (fun i typ ->
                let reader o = readFld (reader o) i
                let name = prefix + "Item" + (i + 1).ToString()
                match Map.tryFind (prefix + name) virtualDbFields with
                | Some project -> (fun _ _ _ -> ()), project prefix
                | None ->
                    if isPrim typ then
                        prim reader primitives.[typ] name false
                    elif isPrimOption typ then
                        primOpt reader primitives.[typ.GenericTypeArguments.[0]]
                            typ name
                    elif not (isRecursive typ) && (isRecord typ || isTuple typ) then
                        subLens typ reader (name + sep)
                    elif not (isRecursive typ) && isUnion typ then 
                        subLens typ reader name
                    else json typ reader name)
            let construct = FSharpValue.PreComputeTupleConstructor typ
            let inject (cols: obj[]) (startidx: int) (v: obj) =
                injectProject |> Array.iter (fun (inj, _) -> inj cols startidx v)
                clearCache ()
            let project r =
                let a = injectProject |> Array.map (fun (_, proj) -> proj r)
                construct a
            (inject, project)
        and createInjectProject (typ: Type) (reader: obj -> obj) prefix =
            let e = "to create a lens from a single column primitive (or serialized type) you must specify a prefix" 
            if not (isRecursive typ) && isPrim typ then
                if prefix = "" then failwith e
                else prim reader primitives.[typ] prefix false
            elif not (isRecursive typ) && isPrimOption typ then
                if prefix = "" then failwith e
                else primOpt reader primitives.[typ.GenericTypeArguments.[0]] typ prefix
            elif not (isRecursive typ) && isRecord typ then record typ reader prefix
            elif not (isRecursive typ) && isUnion typ then union typ reader prefix
            elif not (isRecursive typ) && isTuple typ then tuple typ reader prefix
            else 
                if prefix = "" then failwith e
                else json typ reader prefix
        let (inject, project) = createInjectProject typ id prefix
        lens<'A>(columns.ToArray(),
            (fun cols startidx t -> 
                inject cols startidx (box t)
                vdf |> Map.iter (fun k (id, inject) -> 
                    cols.[startidx + id] <- inject t)),
            (fun r -> project r :?> 'A))

    static member Optional(lens: lens<'A>) : lens<Option<'A>> =
        let Inject cols startidx v =
            match v with
            | None -> ()
            | Some v -> lens.Inject(cols, startidx, v)
        let Project(r: Data.Common.DbDataReader) = 
            let isNull col = r.IsDBNull (ord r col)
            if Array.forall isNull lens.Columns then None
            else Some (lens.Project r)
        ofInjProj lens Inject Project

    static member Tuple(lensA: lens<'A>, lensB: lens<'B>, ?allowedIntersection) = 
        tuple lensA lensB (ai allowedIntersection)

    static member Tuple(lensA: lens<'A>, lensB: lens<'B>, lensC: lens<'C>,
                        ?allowedIntersection) =
        let t = tuple lensA lensB (ai allowedIntersection)
        let t = tuple t lensC (ai allowedIntersection)
        ofInjProj t 
            (fun cols startidx (a, b, c) -> t.Inject(cols, startidx, ((a, b), c)))
            (fun r -> let ((a, b), c) = t.Project r in (a, b, c))

    static member Tuple(lensA: lens<'A>, lensB: lens<'B>, 
                        lensC: lens<'C>, lensD: lens<'D>,
                        ?allowedIntersection) =
        let t = Lens.Tuple(lensA, lensB, lensC, ?allowedIntersection = allowedIntersection)
        let t = Lens.Tuple(t, lensD, ?allowedIntersection = allowedIntersection)
        ofInjProj t 
            (fun cols startidx (a, b, c, d) -> t.Inject(cols, startidx, ((a, b, c), d)))
            (fun r -> let ((a, b, c), d) = t.Project r in (a, b, c, d))

    static member Tuple(lensA: lens<'A>, lensB: lens<'B>, lensC: lens<'C>,
                        lensD: lens<'D>, lensE: lens<'E>,
                        ?allowedIntersection) =
        let t = 
            Lens.Tuple(lensA, lensB, lensC, lensD, 
                ?allowedIntersection = allowedIntersection)
        let t = Lens.Tuple(t, lensE, ?allowedIntersection = allowedIntersection)
        ofInjProj t 
            (fun cols startidx (a, b, c, d, e) -> 
                t.Inject(cols, startidx, ((a, b, c, d), e)))
            (fun r -> let ((a, b, c, d), e) = t.Project r in (a, b, c, d, e))

type parameter<'A> internal (name: String) =
    member __.Name with get() = name
    member __.Type with get() = typeof<'A>

type Parameter =
    static member Create<'A>(name: String) = parameter<'A>(name)
    static member String(name) = Parameter.Create<String>(name)
    static member Int(name) = Parameter.Create<int>(name)
    static member Int64(name) = Parameter.Create<int64>(name)
    static member Float(name) = Parameter.Create<float>(name)
    static member Bool(name) = Parameter.Create<bool>(name)
    static member ByteArray(name) = Parameter.Create<byte[]>(name)
    static member DateTime(name) = Parameter.Create<DateTime>(name)
    static member TimeSpan(name) = Parameter.Create<TimeSpan>(name)

type query<'A, 'B> internal (sql:String, lens:lens<'B>, pars: (String * Type)[],
                             set:DbParameterCollection -> 'A -> unit) =
    let mutable onDispose = []
    interface IDisposable with 
        member __.Dispose() = onDispose |> List.iter (fun f -> f ())
    member o.Dispose() = (o :> IDisposable).Dispose ()
    member val Guid = Guid.NewGuid() with get
    member __.Sql with get() = sql
    member __.Lens with get() = lens
    member __.Parameters with get() = pars
    member internal __.Set(pars, a) = set pars a
    member internal __.PushDispose(f) = onDispose <- f :: onDispose

type Query private () =
    static member Create(sql, lens: lens<'B>) = 
        new query<unit, 'B>(sql, lens, [||], fun _ _ -> ())
    static member Create(sql, lens, p1:parameter<'P1>) =
        new query<_,_>(sql, lens, [|p1.Name, p1.Type|], 
            fun p (p1: 'P1) -> p.[0].Value <- p1)
    static member Create(sql, lens, p1:parameter<'P1>, p2:parameter<'P2>) =
        new query<_,_>(sql, lens, [|p1.Name, p1.Type; p2.Name, p2.Type|],
            fun p (p1: 'P1, p2: 'P2) -> p.[0].Value <- p1; p.[1].Value <- p2)
    static member Create(sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
                         p3:parameter<'P3>) =
        new query<_, _>(sql, lens, 
            [|p1.Name, p1.Type; p2.Name, p2.Type; p3.Name, p3.Type|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3) -> 
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, p3:parameter<'P3>, 
         p4:parameter<'P4>) =
        new query<_, _>(sql, lens, 
            [|p1.Name, p1.Type; p2.Name, p2.Type; p3.Name, p3.Type
              p4.Name, p4.Type|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4) -> 
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>) =
        new query<_, _>(sql, lens, 
            [|p1.Name, p1.Type; p2.Name, p2.Type; p3.Name, p3.Type
              p4.Name, p4.Type; p5.Name, p5.Type|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4, p5: 'P5) -> 
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4
                p.[4].Value <- p5)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>) =
        new query<_,_>(sql, lens, 
            [|p1.Name, p1.Type; p2.Name, p2.Type; p3.Name, p3.Type
              p4.Name, p4.Type; p5.Name, p5.Type; p6.Name, p6.Type|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4, p5: 'P5, p6: 'P6) ->
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4
                p.[4].Value <- p5; p.[5].Value <- p6)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>, p7:parameter<'P7>) =
        new query<_,_>(sql, lens, 
            [|p1.Name, p1.Type; p2.Name, p2.Type; p3.Name, p3.Type
              p4.Name, p4.Type; p5.Name, p5.Type; p6.Name, p6.Type
              p7.Name, p7.Type|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4, p5: 'P5, p6: 'P6, p7: 'P7) ->
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4
                p.[4].Value <- p5; p.[5].Value <- p6
                p.[6].Value <- p7)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>, p7:parameter<'P7>, p8:parameter<'P8>) =
        new query<_,_>(sql, lens, 
            [|p1.Name, p1.Type; p2.Name, p2.Type; p3.Name, p3.Type
              p4.Name, p4.Type; p5.Name, p5.Type; p6.Name, p6.Type
              p7.Name, p7.Type; p8.Name, p8.Type|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4, 
                   p5: 'P5, p6: 'P6, p7: 'P7, p8: 'P8) ->
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4
                p.[4].Value <- p5; p.[5].Value <- p6
                p.[6].Value <- p7; p.[7].Value <- p8)            
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>, p7:parameter<'P7>, p8:parameter<'P8>,
         p9:parameter<'P9>) =
        new query<_,_>(sql, lens, 
            [|p1.Name, p1.Type; p2.Name, p2.Type; p3.Name, p3.Type
              p4.Name, p4.Type; p5.Name, p5.Type; p6.Name, p6.Type
              p7.Name, p7.Type; p8.Name, p8.Type; p9.Name, p9.Type|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4, 
                   p5: 'P5, p6: 'P6, p7: 'P7, p8: 'P8, p9: 'P9) ->
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4
                p.[4].Value <- p5; p.[5].Value <- p6
                p.[6].Value <- p7; p.[7].Value <- p8
                p.[8].Value <- p9)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>, p7:parameter<'P7>, p8:parameter<'P8>,
         p9:parameter<'P9>, p10:parameter<'P10>) =
        new query<_,_>(sql, lens, 
            [|p1.Name, p1.Type; p2.Name, p2.Type; p3.Name, p3.Type
              p4.Name, p4.Type; p5.Name, p5.Type; p6.Name, p6.Type
              p7.Name, p7.Type; p8.Name, p8.Type; p9.Name, p9.Type
              p10.Name, p10.Type|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4, 
                   p5: 'P5, p6: 'P6, p7: 'P7, p8: 'P8, p9: 'P9, p10: 'P10) ->
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4
                p.[4].Value <- p5; p.[5].Value <- p6
                p.[6].Value <- p7; p.[7].Value <- p8
                p.[8].Value <- p9; p.[9].Value <- p10)

type PreparedInsert =
    inherit IDisposable
    abstract member Finish: unit -> unit
    abstract member Row: obj[] -> unit
    abstract member FinishAsync: unit -> Async<unit>
    abstract member RowAsync: obj[] -> Async<unit>

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
        abstract member Query: query<'A, 'B> * 'A -> Async<List<'B>>
        abstract member NonQuery: query<'A, NonQuery> * 'A -> Async<int>
        abstract member Insert: table:String * lens<'A> * seq<'A> -> Async<unit>
        abstract member Transaction: (db -> Async<'a>) -> Async<'a>
        abstract member NoRetry: (db -> Async<'A>) -> Async<'A>

    let getCmd (prepared: Dictionary<Guid, DbCommand>) 
        (con: #DbConnection) (provider: Provider<_,_,_>) (q: query<_,_>)= 
        match prepared.TryFind q.Guid with
        | Some cmd -> cmd 
        | None ->
            let cmd = con.CreateCommand()
            let pars = 
                q.Parameters |> Array.map (fun (name, typ) -> 
                    (provider.CreateParameter(name, typ) :> DbParameter))
            cmd.Connection <- con
            cmd.CommandText <- q.Sql 
            cmd.Parameters.AddRange pars
            cmd.Prepare ()
            prepared.[q.Guid] <- cmd
            q.PushDispose (fun () -> ignore (prepared.Remove q.Guid))
            cmd

    let buildTxn (txn: ref<Option<#DbTransaction>>) (provider: Provider<_,_,_>) 
        (savepoints: Stack<String>) (con: #DbConnection) =
        lock txn (fun () -> 
            let std (t: DbTransaction) =
                let rst () = txn := None; savepoints.Clear ()
                (fun () -> lock txn (fun () -> rst (); t.Rollback ())),
                (fun () -> lock txn (fun () -> rst (); t.Commit ()))
            match !txn with
            | None ->
                let t = provider.BeginTransaction con
                txn := Some t
                std t
            | Some t ->
                if not provider.HasNestedTransactions then std t
                else
                    let name = provider.NestedTransaction(con, t)
                    savepoints.Push name
                    (fun () -> lock txn (fun () -> 
                        provider.RollbackNested(con, t, savepoints.Pop ()))),
                    (fun () -> lock txn (fun () ->
                        provider.CommitNested(con, t, savepoints.Pop()))))

    let prepareInsert (lens: lens<_>) (preparedInserts: Dictionary<_,_>) 
        (provider: Provider<_,_,_>) (table: String) (con: #DbConnection) =
        let len = Array.length lens.Columns
        match preparedInserts.TryFind lens.Guid with
        | Some x -> x
        | None ->
            let pi = provider.PrepareInsert(con, table, lens.Columns)
            let o = Array.create len null
            preparedInserts.[lens.Guid] <- (o, pi)
            (o, pi)

    type Db =
        static member Connect(provider: Provider<_,_,'TXN>) = async {
            let! con = provider.ConnectAsync ()
            let seq = new Sequencer()
            let prepared = Dictionary<Guid, DbCommand>()
            let preparedInserts = Dictionary<Guid, obj[] * PreparedInsert>()
            let txn : ref<Option<'TXN>> = ref None
            let savepoints = Stack<String>()
            return { new db with
                member __.Dispose() = 
                    preparedInserts.Values |> Seq.iter (fun (_, p) -> p.Dispose ())
                    con.Dispose()
                member db.NoRetry(f) = f db
                member __.NonQuery(q, a) = seq.Enqueue (async {
                    let cmd = getCmd prepared con provider q
                    q.Set (cmd.Parameters, a)
                    let! i = cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
                    return i })
                member __.Query(q, a) = seq.Enqueue (async {
                    let read (lens : lens<'B>) (r: DbDataReader) =
                        let l = List<'B>(capacity = 1)
                        let rec loop () = async {
                            let! res = r.ReadAsync() |> Async.AwaitTask
                            if not res then
                                if not r.IsClosed then r.Close ()
                                return l
                            else
                                l.Add (lens.Project r)
                                return! loop () }
                        loop ()
                    let cmd = getCmd prepared con provider q
                    q.Set (cmd.Parameters, a)
                    let! r = cmd.ExecuteReaderAsync() |> Async.AwaitTask
                    return! read q.Lens r })
                member __.Insert(table, lens: lens<'A>, ts: seq<'A>) = 
                    seq.Enqueue(async {
                        let (o, pi) = 
                            prepareInsert lens preparedInserts provider table con
                        do! ts |> Seq.map (fun t -> async {
                                Array.Clear(o, 0, o.Length)
                                lens.Inject(o, 0, t)
                                return! pi.RowAsync o })
                            |> Seq.toList
                            |> Async.sequence
                            |> Async.Ignore
                        return! pi.FinishAsync () })
                member db.Transaction(f) = async {
                    let (rollback, commit) = buildTxn txn provider savepoints con
                    try
                        let! res = f db
                        commit ()
                        return res
                    with e -> rollback (); return raise e } } }

        static member WithRetries(provider: Provider<_,_,_>, ?log, ?tries) = async {
            let log = defaultArg log ignore
            let tries = defaultArg tries 3
            let seq = new Sequencer()
            let mutable lastGoodDb : Option<db> = None
            let connect () = async {
                let! con = Db.Connect(provider)
                lastGoodDb <- Some con
                return con }
            let mutable con : Result<db, exn> = Error (Failure "not connected")
            let mutable disposed : bool = false 
            let mutable safeToRetry : bool = true
            let dispose () = match con with Error _ -> () | Ok o -> try o.Dispose () with _ -> ()
            let reconnect () = async {
                dispose () 
                try
                    let! db = connect ()
                    con <- Ok db
                with e -> con <- Error e }
            let withDb f = seq.Enqueue (async {
                if disposed then failwith "error attempted to use disposed IDb"
                let rec loop i = async {
                    match con with
                    | Ok db -> 
                        try return! f db 
                        with e ->
                            try log e with _ -> ()
                            return! retry i e
                    | Error e -> return! retry i e }
                and retry i e = async { 
                    if i >= tries || not safeToRetry then return raise e 
                    else
                        do! Async.Sleep 1000
                        do! reconnect ()
                        return! loop (i + 1) }
                match con with 
                | Error _ -> do! reconnect () 
                | Ok _ -> ()
                return! loop 0 })
            let rec loop i = async {
                do! reconnect ()
                match con with
                | Error e when i < tries ->
                    try log e with _ -> ()
                    do! Async.Sleep 1000
                    return! loop (i + 1)
                | Error e -> return raise e
                | Ok _ -> () }
            // make sure if we can't connect we fail early
            do! seq.Enqueue(loop 0)
            let getLastGoodDb () = 
                match lastGoodDb with
                | None -> failwith "bug withRetries returned without initializing"
                | Some db -> db
            return { new db with
                member __.NoRetry(f) = withDb (fun db -> async {
                    safeToRetry <- false
                    try return! f db
                    finally safeToRetry <- true })
                member __.Dispose() = 
                    disposed <- true
                    dispose () 
                member __.NonQuery(q, a) = withDb (fun db -> db.NonQuery(q, a))
                member __.Query(q, a) = withDb (fun db -> db.Query(q, a))
                member __.Insert(t, l, a) = withDb (fun db -> db.Insert(t, l, a))
                member __.Transaction(f) = withDb (fun db -> db.Transaction f) } }

type db =
    inherit IDisposable
    abstract member Query: query<'A, 'B> * 'A -> List<'B>
    abstract member NonQuery: query<'A, NonQuery> * 'A -> int
    abstract member Insert: table:String * lens<'A> * seq<'A> -> unit
    abstract member Transaction: (db -> 'a) -> 'a

type Db internal () =
    static member Connect(provider: Provider<_, _, 'TXN>) = 
        let con = provider.Connect ()
        let prepared = Dictionary<Guid, DbCommand>()
        let preparedInserts = Dictionary<Guid, obj[] * PreparedInsert>()
        let txn : ref<Option<'TXN>> = ref None
        let savepoints = Stack<String>()
        { new db with
            member __.Dispose() =
                preparedInserts.Values |> Seq.iter (fun (_, p) -> p.Dispose ())
                con.Dispose()
            member __.NonQuery(q, a) = lock con (fun () -> 
                let cmd = Async.getCmd prepared con provider q
                q.Set (cmd.Parameters, a)
                cmd.ExecuteNonQuery())
            member __.Query(q, a) = lock con (fun () -> 
                let read (lens : lens<'B>) (r: DbDataReader) =
                    let l = List<'B>(capacity = 1)
                    let rec loop () =
                        if not (r.Read ()) then
                            if not r.IsClosed then r.Close ()
                            l
                        else
                            l.Add (lens.Project r)
                            loop ()
                    loop ()
                let cmd = Async.getCmd prepared con provider q
                q.Set (cmd.Parameters, a)
                let r = cmd.ExecuteReader()
                read q.Lens r)
            member __.Insert(table, lens: lens<'A>, a: seq<'A>) = 
                let (o, pi) = Async.prepareInsert lens preparedInserts provider table con
                for t in a do
                    Array.Clear(o, 0, o.Length)
                    lens.Inject(o, 0, t)
                    pi.Row o
                pi.Finish()
            member db.Transaction(f) =
                let (rollback, commit) = Async.buildTxn txn provider savepoints con
                try
                    let res = f db
                    commit ()
                    res
                with e -> rollback (); raise e }
