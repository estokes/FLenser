﻿(*  FLenser, a simple ORM for F#
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
namespace FLenser.Core

open System
open System.Collections.Generic
open System.Data.Common
open FSharpx
open FSharpx.Control
open FSharp.Control
open MBrace.FsPickler
open MBrace.FsPickler.Json
open Microsoft.FSharp.Reflection
open System.Reflection

exception UnexpectedNull

type virtualTypeField<'A> = 'A -> obj
type virtualDbField = String -> String -> DbDataReader -> obj

type json internal (data: String) = member __.Data with get() = data

type internal rawLens = 
    abstract member Columns: String[] with get
    abstract member Types: Type[] with get
    abstract member InjectRaw: obj[] * int * obj -> unit
    abstract member ProjectRaw: DbDataReader -> obj

type lens<'A> internal (columns: String[], types: Type[], paths: array<list<String>>,
                        inject: obj[] -> int -> 'A -> unit, 
                        project: DbDataReader -> 'A) =
    let guid = Guid.NewGuid ()
    member __.Guid with get() = guid
    member __.Columns with get() = columns
    member __.Paths with get() = paths
    member __.Types with get() = types
    member internal __.Inject(target: obj[], startidx:int, v) = inject target startidx v
    member internal __.Project(r) = project r
    interface rawLens with
        member __.Columns with get() = columns
        member __.Types with get() = types
        member o.InjectRaw(target: obj[], startidx: int, v : obj) =
           o.Inject(target, startidx, v :?> 'A)
        member o.ProjectRaw(r) = box (project r)

type NonQuery = {nothing: unit}

module Utils =
    open System.Threading

    type Sequencer () =
        let finished = ref (AsyncResultCell ())
        let locked = ref 0
        let rec loop (f: unit -> Async<'a>) : Async<'a> =
            lock finished (fun () -> 
                if Interlocked.CompareExchange(locked, 1, 0) = 1 then
                    let fin = !finished
                    async {
                        do! fin.AsyncResult
                        return! loop f }
                else
                    let fin = AsyncResultCell()
                    finished := fin
                    async {
                        let unlock () = 
                            let (_: int) = Interlocked.Decrement(locked)
                            fin.RegisterResult (AsyncOk (), reuseThread = true)
                        try
                            let! res = f ()
                            unlock ()
                            return res
                        with e ->
                            unlock ()
                            return raise e })
        member __.Enqueue(job: (unit -> Async<'A>)) : Async<'A> = loop job

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
        let types = Array.append lensA.Types lensB.Types
        let paths = Array.append lensA.Paths lensB.Paths
        lens<'T1 * 'T2>(columns, types, paths,
            (fun cols startidx (v1, v2) -> 
                lensA.Inject(cols, startidx, v1)
                lensB.Inject(cols, startidx + lensA.Columns.Length, v2)),
            (fun r -> (lensA.Project r, lensB.Project r)))

    let ofInjProj (merged: lens<_>) inject project = 
        lens<_>(merged.Columns, merged.Types, merged.Paths, inject, project)

    let ai a = defaultArg a Set.empty

    let NonQuery = {nothing = ()}
    let NonQueryLens =
        let guid = Guid.NewGuid()
        let project (r: DbDataReader) = ignore r
        lens<NonQuery>([||], [||], [||], (fun _ _ _ -> ()), (fun _ -> NonQuery))

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

[<AllowNullLiteralAttribute>]
type FlattenAttribute() =
    inherit Attribute()
    member val Prefix = "" with get, set
    static member Usage =
        "The Flatten attribute may only be applied to record fields and union cases"

[<AllowNullLiteralAttribute>]
type RenameAttribute(columnName: String) = 
    inherit Attribute()
    member __.ColumnName with get() = columnName
    static member Usage =
        "The Rename Attribute may only be applied to record fields and union cases"

type Lens private () =
    static let toplevelPrefixByThread = Dictionary<int, list<String>>()
    static member NonQuery with get() = NonQueryLens
    static member Create<'A>(?virtualDbFields: Map<list<String>, virtualDbField>,
                             ?virtualTypeFields: Map<list<String>, virtualTypeField<'A>>,
                             ?prefix, ?nestingSeparator) : lens<'A> =
        let prefix = defaultArg prefix []
        let toplevelPrefix =
            (* To handle the flatten attribute properly we need to know what prefix was
               passed into the root lens. To be thread safe we store this value indexed
               by thread id. *)
            lock toplevelPrefixByThread (fun () ->
                let id = System.Threading.Thread.CurrentThread.ManagedThreadId
                match toplevelPrefixByThread.TryFind id with
                | None ->
                    // we are the root lens, record the prefix, remove it
                    // when we are done building the lens
                    toplevelPrefixByThread.[id] <- prefix
                    prefix
                | Some pfx -> pfx)
        let sep = defaultArg nestingSeparator ""
        let stringifypath (path: list<String>) = String.Join(sep, path)
        let virtualDbFields = 
            defaultArg virtualDbFields Map.empty
            |> Map.fold (fun m k v ->
                let name = prefix @ k
                Map.add name (stringifypath prefix, stringifypath name, v) m) Map.empty
        let virtualTypeFields = defaultArg virtualTypeFields Map.empty
        let typ = typeof<'A>
        let jsonSer = FsPickler.CreateJsonSerializer(indent = false, omitHeader = true)
        let columns = List(capacity = 10)
        let types = List(capacity = 10)
        let paths = List(capacity = 10)
        let createColumnSlot name typ =
            let id = columns.Count
            paths.Add name
            let name = stringifypath name
            columns.Add name
            types.Add typ
            (name, id)
        let vdf = 
            virtualTypeFields 
            |> Map.map (fun k v ->
                let (_, id) = createColumnSlot (prefix @ k) typeof<obj>
                id, v)
        let prim (inject: obj -> obj) 
            (project: DbDataReader -> String -> obj)
            (name: list<String>) (nullok: bool) typ =
            let (name, colid) = createColumnSlot name typ
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
            (name: list<String>) =
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
            prim inject project name true rtyp
        let json (typ: Type) reader name =
            let pk = FsPickler.GeneratePickler(typ)
            let e = System.Text.Encoding.UTF8
            prim 
                (fun v -> box (json(e.GetString (jsonSer.PickleUntyped(reader v, pk)))))
                (fun r n -> 
                    let s = 
                        match r.GetValue(ord r n) with
                        | :? array<byte> as x -> x
                        | :? String as x -> e.GetBytes x
                        | o -> failwith (sprintf "can't convert %A to byte[]" o)
                    jsonSer.UnPickleUntyped(s, pk, encoding = e))
                name false typeof<json> 
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
                           && ptyp.GenericTypeArguments.[0] = typeof<list<String>>)
                    then 
                        let lens = mi.Invoke(null, [|(Some prefix) :> obj|]) :?> rawLens
                        let startidx = columns.Count
                        columns.AddRange lens.Columns
                        types.AddRange lens.Types
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
                let (flatten, name) =
                    let name =
                        match fld.GetCustomAttribute<RenameAttribute>() with
                        | null -> fld.Name
                        | a -> a.ColumnName
                    match fld.GetCustomAttribute<FlattenAttribute>() with
                    | null -> (false, prefix @ [name])
                    | a -> 
                        match a.Prefix with
                        | "" -> (true, toplevelPrefix)
                        | pfx -> (true, toplevelPrefix @ [pfx])
                let typ = fld.PropertyType
                match Map.tryFind name virtualDbFields with
                | Some (prefix, name, project) -> (fun _ _ _ -> ()), project prefix name
                | None ->
                    if isPrim typ then
                        if flatten then 
                            failwith "primitive record fields may not be flattened"
                        prim reader primitives.[typ] name false typ
                    elif isPrimOption typ then
                        if flatten then 
                            failwith "primitive option record fields may not be flattened"
                        primOpt reader primitives.[typ.GenericTypeArguments.[0]]
                            typ name
                    elif not (isRecursive typ) &&
                         (isRecord typ || isTuple typ || isUnion typ) then 
                        subLens typ reader name
                    else
                        if flatten then failwith "json record fields may not be flattened"
                        json typ reader name)
            let construct = FSharpValue.PreComputeRecordConstructor(typ, true)
            let inject cols startidx record = 
                injectAndProject |> Array.iter (fun (inj, _) -> inj cols startidx record)
            let project r =
                let a = injectAndProject |> Array.map (fun (_, proj) -> proj r)
                construct a
            (inject, project)
        and union (typ: Type) reader prefix =
            let prefix = if prefix = [] then [typ.Name] else prefix
            let (basename, colid) = createColumnSlot prefix typeof<int>
            let tag = FSharpValue.PreComputeUnionTagReader(typ, true)
            let mutable clearPrecomputedCache = []
            let case (c: UnionCaseInfo) =
                let fields = c.GetFields ()
                let casename = 
                    c.GetCustomAttributes()
                    |> Seq.tryPick (function
                        | :? RenameAttribute as a -> Some a.ColumnName
                        | _ -> None)
                    |> Option.getOrElse c.Name
                let flatten = 
                    c.GetCustomAttributes()
                    |> Seq.tryPick (function
                        | :? FlattenAttribute as a -> Some a
                        | _ -> None)
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
                            let name =
                                let singletonRecord =
                                    fields.Length = 1 
                                    && (isRecord typ || isTuple typ || isUnion typ)
                                match flatten with
                                | None when singletonRecord -> prefix @ [casename]
                                | None -> prefix @ [casename; fld.Name]
                                | Some a -> 
                                    match a.Prefix with 
                                    | "" when singletonRecord -> toplevelPrefix
                                    | "" -> toplevelPrefix @ [fld.Name]
                                    | p when singletonRecord -> toplevelPrefix @ [p]
                                    | p -> toplevelPrefix @ [p; fld.Name]
                            match Map.tryFind name virtualDbFields with
                            | Some (prefix, name, project) -> 
                                (fun _ _ _ -> ()), project prefix name
                            | None ->
                                if isPrim typ then
                                    prim reader primitives.[typ] name false typ
                                elif isPrimOption typ then
                                    primOpt reader 
                                        primitives.[typ.GenericTypeArguments.[0]]
                                        typ name
                                elif not (isRecursive typ) 
                                     && (isRecord typ || isTuple typ || isUnion typ) then 
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
                let i = objToInt32 (r.GetValue(ord r basename))
                let (mk, (_, proj)) = mk.[i]
                mk (proj r)
            (inject, project)
        and tuple (typ: Type) reader prefix =
            let flds = FSharpType.GetTupleElements typ
            let (readFld, clearCache) = 
                readFldFromPrecomputed (FSharpValue.PreComputeTupleReader typ)
            let injectProject = flds |> Array.mapi (fun i typ ->
                let reader o = readFld (reader o) i
                let name = prefix @ ["Item" + (i + 1).ToString()]
                match Map.tryFind name virtualDbFields with
                | Some (prefix, name, project) -> (fun _ _ _ -> ()), project prefix name
                | None ->
                    if isPrim typ then
                        prim reader primitives.[typ] name false typ
                    elif isPrimOption typ then
                        primOpt reader primitives.[typ.GenericTypeArguments.[0]]
                            typ name
                    elif not (isRecursive typ) 
                         && (isRecord typ || isTuple typ || isUnion typ) then
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
            match typ.GetCustomAttribute<FlattenAttribute>() with
            | null -> ()
            | _ -> failwith FlattenAttribute.Usage
            match typ.GetCustomAttribute<RenameAttribute>() with
            | null -> ()
            | _ -> failwith RenameAttribute.Usage
            let singletonPrefix = if prefix <> [] then prefix else ["Item"]
            if not (isRecursive typ) && isPrim typ then
                prim reader primitives.[typ] singletonPrefix false typ
            elif not (isRecursive typ) && isPrimOption typ then
                primOpt reader primitives.[typ.GenericTypeArguments.[0]] typ 
                    singletonPrefix
            elif not (isRecursive typ) && isRecord typ then record typ reader prefix
            elif not (isRecursive typ) && isUnion typ then union typ reader prefix
            elif not (isRecursive typ) && isTuple typ then tuple typ reader prefix
            else json typ reader prefix
        let (inject, project) = createInjectProject typ id prefix
        let cols = columns.ToArray()
        if Array.distinct cols <> cols then
            failwith (sprintf "FLenser naming conflict %s" (String.Join(", ", cols)))
        lock toplevelPrefixByThread (fun () ->
            let id = System.Threading.Thread.CurrentThread.ManagedThreadId
            match toplevelPrefixByThread.TryFind id with
            | None -> failwith (sprintf "bug no toplevel prefix found for tid %d" id)
            | Some pfx when pfx = prefix -> toplevelPrefixByThread.Remove id |> ignore
            | Some _ -> ())
        lens<'A>(cols, types.ToArray(), paths.ToArray(),
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

type parameter<'A> =
    | Single of name:String * typ:Type
    | FromLens of lens<'A> * names:String[] * vals:obj[]
    member p.Pars = 
        match p with
        | Single (name, typ) -> [|name, typ|]
        | FromLens (l, names, _) -> Array.zip names l.Types
    member p.Inject (c: DbParameterCollection) (v: 'A) (i: int) : int =
        let map (v: obj) =
            match v with
            | null -> box DBNull.Value
            | :? json as x -> box x.Data
            | _ -> v
        match p with
        | Single _ -> 
            c.[i].Value <- map (box v)
            i + 1
        | FromLens (l, _, vals) -> 
            Array.Clear(vals, 0, vals.Length)
            l.Inject(vals, 0, v)
            vals |> Array.iteri (fun j v -> c.[i + j].Value <- map v)
            i + vals.Length

type Parameter =
    static member private Create<'A>(name: String) = 
        Single (name, typeof<'A>) : parameter<'A>
    static member String(name) = Parameter.Create<String>(name)
    static member Int(name) = Parameter.Create<int>(name)
    static member Int64(name) = Parameter.Create<int64>(name)
    static member Float(name) = Parameter.Create<float>(name)
    static member Bool(name) = Parameter.Create<bool>(name)
    static member DateTime(name) = Parameter.Create<DateTime>(name)
    static member TimeSpan(name) = Parameter.Create<TimeSpan>(name)
    static member Array(p: parameter<'A>) =
        match p with
        | Single (name, _) -> Parameter.Create<'A[]>(name)
        | FromLens _ -> 
            failwith "Parameter.Create cannot create an array parameter from an OfLens"
    static member OfLens(lens: lens<'A>, ?paramNestingSep, ?namePrefix) : parameter<'A> =
        let pns = defaultArg paramNestingSep ""
        let pfx = defaultArg namePrefix ""
        let names = lens.Paths |> Array.map (fun p -> pfx + String.concat pns p)
        FromLens (lens, names, Array.create lens.Columns.Length null)

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
        new query<_,_>(sql, lens, p1.Pars, 
            fun p (p1v: 'P1) -> p1.Inject p p1v 0 |> ignore)
    static member Create(sql, lens, p1:parameter<'P1>, p2:parameter<'P2>) =
        new query<_,_>(sql, lens, Array.concat [|p1.Pars; p2.Pars|],
            fun p (p1v: 'P1, p2v: 'P2) -> 
                p1.Inject p p1v 0 |> p2.Inject p p2v |> ignore)
    static member Create(sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
                         p3:parameter<'P3>) =
        new query<_, _>(sql, lens, 
            Array.concat [|p1.Pars; p2.Pars; p3.Pars|],
            fun p (p1v: 'P1, p2v: 'P2, p3v: 'P3) ->
                p1.Inject p p1v 0 |> p2.Inject p p2v |> p3.Inject p p3v |> ignore)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, p3:parameter<'P3>, 
         p4:parameter<'P4>) =
        new query<_, _>(sql, lens, 
            Array.concat [|p1.Pars; p2.Pars; p3.Pars; p4.Pars|],
            fun p (p1v: 'P1, p2v: 'P2, p3v: 'P3, p4v: 'P4) ->
                p1.Inject p p1v 0 |> p2.Inject p p2v |> p3.Inject p p3v 
                |> p4.Inject p p4v |> ignore)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>) =
        new query<_, _>(sql, lens, 
            Array.concat [|p1.Pars; p2.Pars; p3.Pars; p4.Pars; p5.Pars|],
            fun p (p1v: 'P1, p2v: 'P2, p3v: 'P3, p4v: 'P4, p5v: 'P5) ->
                p1.Inject p p1v 0 |> p2.Inject p p2v |> p3.Inject p p3v
                |> p4.Inject p p4v |> p5.Inject p p5v |> ignore)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>) =
        new query<_,_>(sql, lens, 
            Array.concat [|p1.Pars; p2.Pars; p3.Pars; p4.Pars; p5.Pars; p6.Pars|],
            fun p (p1v: 'P1, p2v: 'P2, p3v: 'P3, p4v: 'P4, p5v: 'P5, p6v: 'P6) ->
                p1.Inject p p1v 0 |> p2.Inject p p2v |> p3.Inject p p3v
                |> p4.Inject p p4v |> p5.Inject p p5v |> p6.Inject p p6v
                |> ignore)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>, p7:parameter<'P7>) =
        new query<_,_>(sql, lens, 
            [|p1.Pars; p2.Pars; p3.Pars; p4.Pars; p5.Pars; p6.Pars; p7.Pars|]
            |> Array.concat,
            fun p (p1v: 'P1, p2v: 'P2, p3v: 'P3, p4v: 'P4, p5v: 'P5, p6v: 'P6, p7v: 'P7) ->
                p1.Inject p p1v 0 |> p2.Inject p p2v |> p3.Inject p p3v
                |> p4.Inject p p4v |> p5.Inject p p5v |> p6.Inject p p6v
                |> p7.Inject p p7v |> ignore)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>, p7:parameter<'P7>, p8:parameter<'P8>) =
        new query<_,_>(sql, lens, 
            [|p1.Pars; p2.Pars; p3.Pars; p4.Pars; p5.Pars; p6.Pars; p7.Pars; p8.Pars|]
            |> Array.concat,
            fun p (p1v: 'P1, p2v: 'P2, p3v: 'P3, p4v: 'P4, 
                   p5v: 'P5, p6v: 'P6, p7v: 'P7, p8v: 'P8) ->
                p1.Inject p p1v 0 |> p2.Inject p p2v |> p3.Inject p p3v
                |> p4.Inject p p4v |> p5.Inject p p5v |> p6.Inject p p6v
                |> p7.Inject p p7v |> p8.Inject p p8v |> ignore)            
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>, p7:parameter<'P7>, p8:parameter<'P8>,
         p9:parameter<'P9>) =
        new query<_,_>(sql, lens, 
            [|p1.Pars; p2.Pars; p3.Pars; p4.Pars; p5.Pars; p6.Pars
              p7.Pars; p8.Pars; p9.Pars|]
            |> Array.concat,
            fun p (p1v: 'P1, p2v: 'P2, p3v: 'P3, p4v: 'P4, 
                   p5v: 'P5, p6v: 'P6, p7v: 'P7, p8v: 'P8, p9v: 'P9) ->
                p1.Inject p p1v 0 |> p2.Inject p p2v |> p3.Inject p p3v
                |> p4.Inject p p4v |> p5.Inject p p5v |> p6.Inject p p6v
                |> p7.Inject p p7v |> p8.Inject p p8v |> p9.Inject p p9v
                |> ignore)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>, p7:parameter<'P7>, p8:parameter<'P8>,
         p9:parameter<'P9>, p10:parameter<'P10>) =
        new query<_,_>(sql, lens, 
            [|p1.Pars; p2.Pars; p3.Pars; p4.Pars; p5.Pars; p6.Pars
              p7.Pars; p8.Pars; p9.Pars; p10.Pars|]
            |> Array.concat,
            fun p (p1v: 'P1, p2v: 'P2, p3v: 'P3, p4v: 'P4, 
                   p5v: 'P5, p6v: 'P6, p7v: 'P7, p8v: 'P8, p9v: 'P9, p10v: 'P10) ->
                p1.Inject p p1v 0 |> p2.Inject p p2v |> p3.Inject p p3v
                |> p4.Inject p p4v |> p5.Inject p p5v |> p6.Inject p p6v
                |> p7.Inject p p7v |> p8.Inject p p8v |> p9.Inject p p9v
                |> p10.Inject p p10v |> ignore)

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
        abstract member Query: query<'A, 'B> * 'A -> Async<AsyncSeq<'B>>
        abstract member QuerySingle: query<'A, 'B> * 'A -> Async<Option<'B>>
        abstract member CancelQuery: unit -> Async<unit>
        abstract member NonQuery: query<'A, NonQuery> * 'A -> Async<int>
        abstract member Compile: query<_, _> -> Async<unit>
        abstract member Insert: table:String * lens<'A> * seq<'A> -> Async<unit>
        abstract member Transaction: (db -> Async<'a>) -> Async<'a>
        abstract member NoRetry: (db -> Async<'A>) -> Async<'A>

    let toOpt (q: query<_,_>) (l: 'A[]) : Option<'A> =
        if l.Length = 0 then None
        elif l.Length = 1 then Some l.[0]
        else failwith (sprintf "expected 0 or 1 result from query %s" q.Sql)

    let getCmd (prepared: Dictionary<Guid, DbCommand>) 
        (con: #DbConnection) (provider: Provider<_,_,_>) (q: query<_,_>)= 
        match prepared.TryFind q.Guid with
        | Some cmd -> cmd 
        | None ->
            let cmd = con.CreateCommand()
            let pars = 
                q.Parameters |> Array.map (fun (name, typ) -> 
                    let p = (provider.CreateParameter(name, typ) :> DbParameter)
                    p.IsNullable <- true
                    p)
            cmd.Connection <- con
            cmd.CommandText <- q.Sql 
            cmd.Parameters.AddRange pars
            try cmd.Prepare ()
            with exn -> 
                failwith (sprintf "error preparing stmt %s error %A" cmd.CommandText exn)
            prepared.[q.Guid] <- cmd
            q.PushDispose (fun () -> ignore (prepared.Remove q.Guid))
            cmd

    let buildTxn (txn: ref<Option<#DbTransaction>>) (provider: Provider<_,_,_>) 
        (savepoints: Stack<String>) (con: #DbConnection) =
        lock txn (fun () -> 
            let std (t: DbTransaction) =
                let rst () = txn := None; savepoints.Clear ()
                (fun () -> lock txn (fun () -> 
                    rst ()
                    match t.Connection with
                    | null -> ()
                    | _ -> t.Rollback ())),
                (fun () -> lock txn (fun () -> 
                    rst ()
                    match t.Connection with
                    | null -> ()
                    | _ -> t.Commit ()))
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
            let currentQuery : ref<Option<DbDataReader * String>> = ref None
            return { new db with
                member __.Dispose() = 
                    preparedInserts.Values |> Seq.iter (fun (_, p) -> p.Dispose ())
                    con.Dispose()
                member db.NoRetry(f) = f db
                member __.NonQuery(q, a) = seq.Enqueue (fun () -> async {
                    let cmd = getCmd prepared con provider q
                    q.Set (cmd.Parameters, a)
                    let! i = cmd.ExecuteNonQueryAsync() |> Async.AwaitTask
                    return i })
                member __.Query(q, a) = seq.Enqueue (fun () -> async {
                    let read (lens : lens<'B>) (r: DbDataReader) =
                        let rec loop () = asyncSeq {
                            let! res = r.ReadAsync() |> Async.AwaitTask
                            if not res then
                                if not r.IsClosed then r.Close ()
                                currentQuery := None
                            else
                                yield lens.Project r
                                yield! loop () }
                        loop ()
                    match !currentQuery with
                    | None -> ()
                    | Some (_, q) -> failwithf "A Query is already in progress (%s)" q
                    let cmd = getCmd prepared con provider q
                    q.Set (cmd.Parameters, a)
                    let! r = cmd.ExecuteReaderAsync() |> Async.AwaitTask
                    currentQuery := Some (r, q.Sql)
                    return read q.Lens r })
                member db.QuerySingle(q, a) = async {
                    let! res = db.Query(q, a)
                    let! res = AsyncSeq.toArrayAsync res
                    return toOpt q res }
                member __.CancelQuery() = 
                    match !currentQuery with
                    | None -> ()
                    | Some (r, _) -> try r.Close() with _ -> ()
                    Async.unit
                member __.Compile(q) = seq.Enqueue (fun () ->
                    let (_: DbCommand) = getCmd prepared con provider q
                    Async.unit)
                member __.Insert(table, lens: lens<'A>, ts: seq<'A>) = 
                    seq.Enqueue(fun () -> async {
                        let (o, pi) = 
                            prepareInsert lens preparedInserts provider table con
                        if Seq.isEmpty ts then return ()
                        else
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
                        do! db.CancelQuery()
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
                let q = Query.Create("select 1 as c", Lens.Create<int>(prefix = ["c"]))
                let! _ = con.Query(q, ())
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
            let withDb f = seq.Enqueue (fun () -> async {
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
            do! seq.Enqueue(fun () -> loop 0)
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
                member __.Compile(q) = withDb (fun db -> db.Compile(q))
                member db.QuerySingle(q, a) = async {
                    // don't retry if toOpt fails due to more than one result
                    let! res = db.Query(q, a)
                    let! res = AsyncSeq.toArrayAsync res
                    return toOpt q res }
                member __.CancelQuery() = withDb (fun db -> db.CancelQuery())
                member __.Insert(t, l, a) = withDb (fun db -> db.Insert(t, l, a))
                member __.Transaction(f) = withDb (fun db -> db.Transaction f) } }

type db =
    inherit IDisposable
    abstract member Query: query<'A, 'B> * 'A -> seq<'B>
    abstract member QuerySingle: query<'A, 'B> * 'A -> Option<'B>
    abstract member CancelQuery: unit -> unit
    abstract member Compile: query<_, _> -> unit
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
        let currentQuery : ref<Option<DbDataReader * String>> = ref None
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
                    let rec loop () = seq {
                        if not (r.Read ()) then
                            if not r.IsClosed then r.Close ()
                            currentQuery := None
                        else
                            yield lens.Project r
                            yield! loop () }
                    loop ()
                match !currentQuery with
                | None -> ()
                | Some (_, q) -> failwithf "a Query is already in progress (%s)" q
                let cmd = Async.getCmd prepared con provider q
                q.Set (cmd.Parameters, a)
                let r = cmd.ExecuteReader()
                currentQuery := Some (r, q.Sql)
                read q.Lens r)
            member __.Compile(q) = 
                ignore (Async.getCmd prepared con provider q : DbCommand)
            member db.QuerySingle(q, a) = db.Query(q, a) |> Seq.toArray |> Async.toOpt q
            member __.CancelQuery() =
                match !currentQuery with
                | None -> ()
                | Some (r, _) -> try r.Close() with _ -> ()
            member __.Insert(table, lens: lens<'A>, a: seq<'A>) = 
                let (o, pi) = Async.prepareInsert lens preparedInserts provider table con
                if not <| Seq.isEmpty a then
                    for t in a do
                        Array.Clear(o, 0, o.Length)
                        lens.Inject(o, 0, t)
                        pi.Row o
                    pi.Finish()
            member db.Transaction(f) =
                let (rollback, commit) = Async.buildTxn txn provider savepoints con
                try
                    let res = f db
                    db.CancelQuery()
                    commit ()
                    res
                with e -> rollback (); raise e }
