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

type virtualRecordField<'A> = 'A -> obj
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

open Utils

type CreateSubLensAttribute() = inherit Attribute()

type Lens =
    static member NonQuery with get() = NonQueryLens
    static member Create<'A>(?virtualDbFields: Map<String, virtualDbField>,
                             ?virtualRecordFields: Map<String, virtualRecordField<'A>>,
                             ?prefix) : lens<'A> =
        let prefix = defaultArg prefix ""
        let virtualDbFields = 
            defaultArg virtualDbFields Map.empty
            |> Map.fold (fun m k v -> Map.add (prefix + k) v m) Map.empty
        let virtualRecordFields = defaultArg virtualRecordFields Map.empty
        let typ = typeof<'A>
        let jsonSer = FsPickler.CreateJsonSerializer(indent = false, omitHeader = true)
        let columns = List<_>(capacity = 10)
        let createColumnSlot name =
            let id = columns.Count
            columns.Add name
            id
        let vdf = 
            virtualRecordFields 
            |> Map.map (fun k v -> createColumnSlot (prefix + k), v)
        let rec subRecord (typ: Type) reader prefix =
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
                    if ret.GUID = typeof<lens<_>>.GUID 
                       && mi.IsStatic
                       && ret.GenericTypeArguments.[0] = typ
                       && Array.length pars = 1
                       && (let (p, ptyp) = pars.[0], pars.[0].ParameterType
                           p.Name = "prefix"
                           && ptyp.GUID = typeof<Option<_>>.GUID
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
        and createInjectProject (typ: Type) (reader: obj -> obj) prefix =
            let option (typ : Type) f =
                let ucases = FSharpType.GetUnionCases(typ)
                let none = ucases |> Array.find (fun c -> c.Name = "None")
                let some = ucases |> Array.find (fun c -> c.Name = "Some")
                let get = FSharpValue.PreComputeUnionReader(some)
                let get (o : obj) = (get o).[0]
                let tag = FSharpValue.PreComputeUnionTagReader(typ)
                let mkNone = FSharpValue.PreComputeUnionConstructor none
                let mkSome = FSharpValue.PreComputeUnionConstructor some
                let rtyp = typ.GenericTypeArguments.[0]
                f none some get tag mkNone mkSome rtyp
            let prim (inject: obj -> obj) 
                (project: DbDataReader -> String -> obj)
                (fld : Reflection.PropertyInfo)
                (nullok: bool) =
                let name = prefix + fld.Name
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
                (inject: obj -> obj)
                (project : DbDataReader -> String -> obj) 
                (fld : Reflection.PropertyInfo) =
                option fld.PropertyType (fun none some get tag mkNone mkSome _ ->
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
                    prim inject project fld true)
            let getp (r: DbDataReader) name f = f (ord r name)
            let primitives = 
                let std r n = getp r n r.GetValue
                [ typeof<Boolean>.GUID, 
                    (fun r n -> getp r n (fun i ->
                        match r.GetValue i with
                        | :? bool as x -> x
                        | :? byte as x -> x > 0uy
                        | :? int16 as x -> x > 0s
                        | :? int32 as x -> x > 0
                        | :? int64 as x -> x > 0L
                        | o -> failwith (sprintf "can't cast %A to boolean" o)
                        |> box))
                  typeof<Double>.GUID, std
                  typeof<String>.GUID, std
                  typeof<DateTime>.GUID, 
                    (fun r n -> getp r n (fun i ->
                        match r.GetValue i with
                        | :? DateTime as x -> x
                        | :? String as x -> DateTime.Parse x
                        | o -> failwith (sprintf "can't cast %A to DateTime" o)
                        |> box))
                  typeof<TimeSpan>.GUID, 
                    (fun r n -> getp r n (fun i ->
                        match r.GetValue i with
                        | :? TimeSpan as x -> x
                        | :? String as x -> TimeSpan.Parse x
                        | o -> failwith (sprintf "can't cast %A to TimeSpan" o)
                        |> box))
                  typeof<byte[]>.GUID, std
                  typeof<int>.GUID, 
                    (fun r n -> getp r n (fun i ->
                        match r.GetValue i with
                        | :? int32 as x -> x
                        | :? byte as x -> int32 x
                        | :? int16 as x -> int32 x
                        | :? int64 as x -> int32 x
                        | o -> failwith (sprintf "cannot convert %A to int32" o)
                        |> box))
                  typeof<int64>.GUID, 
                    (fun r n -> getp r n (fun i ->
                        match r.GetValue i with
                        | :? int64 as x -> x
                        | :? byte as x -> int64 x
                        | :? int16 as x -> int64 x
                        | :? int32 as x -> int64 x
                        | o -> failwith (sprintf "cannot convert %A to int64" o)
                        |> box)) ]
                |> Map.ofList
            let fields = 
                FSharpType.GetRecordFields
                    (typ, allowAccessToPrivateRepresentation = true)
            let injectAndProject = fields |> Array.map (fun fld -> 
                let reader (o : obj) = 
                    FSharpValue.PreComputeRecordFieldReader fld (reader o)
                let name = fld.Name
                match Map.tryFind (prefix + name) virtualDbFields with
                | Some project -> (fun _ _ _ -> ()), project prefix
                | None ->
                    match fld.PropertyType with
                    | typ when FSharpType.IsRecord(typ) ->
                        subRecord typ reader (prefix + fld.Name + "$")
                    | typ when Map.containsKey typ.GUID primitives ->
                        prim reader primitives.[typ.GUID] fld false
                    | typ when typ.GUID = typeof<Option<_>>.GUID 
                               && Map.containsKey typ.GenericTypeArguments.[0].GUID 
                                    primitives ->
                        primOpt reader id 
                            primitives.[typ.GenericTypeArguments.[0].GUID] fld
                    | typ when typ.IsArray && typ.HasElementType
                               && Map.containsKey (typ.GetElementType()).GUID primitives ->
                        prim reader (fun r n -> getp r n r.GetValue) fld false
                    | typ when typ.GUID = typeof<Option<_>>.GUID
                               && let ityp = typ.GenericTypeArguments.[0]
                                  ityp.IsArray && ityp.HasElementType
                                  && Map.containsKey (ityp.GetElementType()).GUID primitives ->
                        primOpt reader id (fun r n -> getp r n r.GetValue) fld
                    | typ when FSharpType.IsUnion(typ, true)
                               && typ.GenericTypeArguments = [||]
                               && (FSharpType.GetUnionCases(typ)
                                  |> Array.forall (fun c ->
                                       match c.GetFields () with
                                       | [||] -> true
                                       | [|ifo|] -> FSharpType.IsRecord(ifo.PropertyType)
                                       | _ -> false)) ->
                        let name = prefix + fld.Name
                        let colid = createColumnSlot name
                        let tag = FSharpValue.PreComputeUnionTagReader(typ)
                        let mutable subrecprefixes = []
                        let subRecord (c: UnionCaseInfo) =
                            match c.GetFields() with
                            | [||] -> None
                            | [|ifo|] ->
                                let readFlds = FSharpValue.PreComputeUnionReader(c, true)
                                let reader (o: obj) = (readFlds (reader o)).[0]
                                let t = ifo.PropertyType
                                let prefix = prefix + fld.Name + "$" + c.Name + "$"
                                subrecprefixes <- prefix :: subrecprefixes
                                Some (subRecord t reader prefix)
                            | _ -> failwith "bug"
                        let mk =
                            let d = Dictionary<_,_>()
                            FSharpType.GetUnionCases(typ) 
                            |> Array.map (fun c ->
                                c.Tag, (FSharpValue.PreComputeUnionConstructor(c),
                                        subRecord c))
                            |> Array.iter (fun (k, v) -> d.[k] <- v)
                            d
                        let inject (cols: obj[]) startidx v =
                            let tag = tag (reader v)
                            cols.[startidx + colid] <- box tag
                            let (_mk, subrec) = mk.[tag]
                            match subrec with
                            | None -> ()
                            | Some (inj, _proj) -> inj cols startidx v
                        let project (r: DbDataReader) =
                            let i = r.GetInt32(ord r name)
                            let (mk, subrec) = mk.[i]
                            match subrec with
                            | None -> mk [||]
                            | Some (_, proj) -> mk [|proj r|]
                        (inject, project)
                    | typ ->
                        let pk = FsPickler.GeneratePickler(typ)
                        let e = System.Text.Encoding.UTF8
                        prim 
                            (fun v -> 
                                box (e.GetString (jsonSer.PickleUntyped(reader v, pk))))
                            (fun r n -> 
                                let s = r.GetString(ord r n)
                                jsonSer.UnPickleUntyped(e.GetBytes s, pk, encoding = e))
                            fld false)
            let construct = 
                FSharpValue.PreComputeRecordConstructor
                    (typ, allowAccessToPrivateRepresentation = true)
            let inject cols startidx record = 
                injectAndProject |> Array.iter (fun (inj, _) -> inj cols startidx record)
            let project r =
                let a = injectAndProject |> Array.map (fun (_, proj) -> proj r)
                construct a
            (inject, project)
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
                        let l = List<'B>(capacity = 4)
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
                    let l = List<'B>(capacity = 4)
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
