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

type virtualRecordField<'A> = DbParameter -> 'A -> unit
type virtualDbField = String -> DbDataReader -> obj

type lens<'A> internal (createParams: (String -> DbParameter) 
                                      -> Dictionary<Guid, DbParameter>,
                        columnNames : String[], 
                        inject: Dictionary<Guid, DbParameter> -> 'A -> unit, 
                        project: DbDataReader -> 'A) =
    let guid = Guid.NewGuid ()
    member __.Guid with get() = guid
    member __.ColumnNames with get() = columnNames
    member internal __.CreateParams(create: String -> DbParameter) = createParams create
    member internal __.Inject(p, v) = inject p v
    member internal __.Project(r) = project r

type NonQuery = {nothing: unit}

module Utils =
    type Sequencer() =
        let mutable stop = false
        let mutable available = AsyncResultCell()
        let jobs = Queue<Async<Unit>>()
        let rec loop () = async {
            if stop then ()
            else
                do! available.AsyncResult
                let job =
                    lock jobs (fun () -> 
                        if jobs.Count = 0 then
                            available <- AsyncResultCell()
                            None
                        else Some (jobs.Dequeue ()))
                match job with
                | None -> return! loop ()
                | Some job ->
                    do! job 
                    return! loop () }
        do loop () |> Async.StartImmediate
        interface IDisposable with member __.Dispose() = stop <- true
        member __.Enqueue(job : Async<'A>) : Async<'A> =
            if stop then failwith "The sequencer is stopped!"
            let res = AsyncResultCell()
            let job = async {
                try
                    let! z = job
                    res.RegisterResult (AsyncOk z)
                with e -> 
                    return res.RegisterResult (AsyncException e) }
            lock jobs (fun () ->
                if jobs.Count = 0 then available.RegisterResult (AsyncOk ())
                jobs.Enqueue(job))
            res.AsyncResult

    type Result<'A, 'B> =
        | Ok of 'A
        | Error of 'B

    let ord (r: DbDataReader) name = 
        try r.GetOrdinal name
        with e -> failwith (sprintf "could not find field %s %A" name e)

    let tuple (lensA: lens<'T1>) (lensB: lens<'T2>) allowedIntersection : lens<'T1 * 'T2> =
        let c (l : lens<_>) = l.ColumnNames |> Set.ofArray
        let is = Set.difference (Set.intersect (c lensA) (c lensB)) allowedIntersection
        if not (Set.isEmpty is) then
            failwith (sprintf "column names have a non empty intersection %A" is)
        let createParams f = 
            let pA = lensA.CreateParams f
            let pB = lensB.CreateParams f
            for kv in pB do pA.[kv.Key] <- kv.Value
            pA
        let columnNames = Array.append lensA.ColumnNames lensB.ColumnNames
        lens<'T1 * 'T2>(createParams, columnNames, 
            (fun p (v1, v2) -> lensA.Inject(p, v1); lensB.Inject(p, v2)),
            (fun r -> (lensA.Project r, lensB.Project r)))

    let ofInjProj (merged : lens<_>) inject project =
        lens<_>((fun c -> merged.CreateParams c), merged.ColumnNames, inject, project)

    let ai a = defaultArg a Set.empty

    let NonQuery = {nothing = ()}
    let NonQueryLens =
        let guid = Guid.NewGuid()
        let project (r: DbDataReader) = ignore r
        lens<NonQuery>((fun _ -> Dictionary<_,_>()), [||], 
            (fun _ _ -> ()), 
            (fun _ -> NonQuery))

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
        let mutable parameters = Dictionary<_,_>()
        let paramsByPrefix = Dictionary<_, _>()
        let createParamSlot prefix name =
            let id = Guid.NewGuid ()
            parameters.[id] <- name
            match paramsByPrefix.TryFind prefix with
            | None -> paramsByPrefix.[prefix] <- [|id|]
            | Some a -> paramsByPrefix.[prefix] <- Array.append a [|id|]
            id
        let vdf = 
            virtualRecordFields 
            |> Map.map (fun k v -> createParamSlot prefix (prefix + k), v)
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
                        let lens = mi.Invoke(null, [|(Some prefix) :> obj|]) :?> lens<_>
                        ((fun p o -> lens.Inject(p, reader o)), lens.Project)
                    else failwith 
                            (sprintf "CreateSubLens marked method does not 
                                      match the required signature
                                      static member _: ?prefix:String -> lens<'A>
                                      %A" mi)
                | _ -> failwith (sprintf "multiple CreateSubLens in %A" typ)
        and createInjectProject (typ : Type) (reader: obj -> obj) prefix =
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
            let prim (inject: Dictionary<_,_> -> DbParameter -> obj -> unit) 
                (project: DbDataReader -> String -> obj)
                (fld : Reflection.PropertyInfo)
                (nullok: bool) =
                let name = prefix + fld.Name
                let paramid = createParamSlot prefix name
                let inject (p: Dictionary<Guid, DbParameter>) (record : obj) : unit = 
                    inject p p.[paramid] record
                let project (r: DbDataReader) : obj = 
                    let o = project r name
                    match o with
                    | null when not nullok -> raise UnexpectedNull
                    | o -> o
                (inject, project)
            let primOpt (reader : obj -> obj) 
                (inject : Dictionary<_,_> -> DbParameter -> obj -> unit)
                (project : DbDataReader -> String -> obj) 
                (fld : Reflection.PropertyInfo) =
                option fld.PropertyType (fun none some get tag mkNone mkSome _ ->
                    let inject pars (p : DbParameter) (v : obj) =
                        let o = reader v
                        let tag = tag o
                        if tag = none.Tag then p.Value <- null
                        else if tag = some.Tag then inject pars p (get o)
                        else failwith "bug"
                    let project (r: DbDataReader) name : obj =
                        if r.IsDBNull(ord r name) then mkNone [||]
                        else 
                            let o = project r name
                            mkSome [|o|]
                    prim inject project fld true)
            let getp (r: DbDataReader) name f = f (ord r name)
            let primitives = 
                let stdInj _ (p: DbParameter) v = p.Value <- box v
                let stdProj r n = getp r n r.GetValue
                let std = (stdInj, stdProj)
                [ typeof<Boolean>.GUID, std
                  typeof<Double>.GUID, std
                  typeof<String>.GUID, std
                  typeof<DateTime>.GUID, std
                  typeof<TimeSpan>.GUID, std
                  typeof<byte[]>.GUID, std
                  typeof<int>.GUID, 
                    (stdInj,
                     fun r n -> getp r n (fun i ->
                        match r.GetValue i with
                        | :? int32 as x -> x
                        | :? byte as x -> int32 x
                        | :? int16 as x -> int32 x
                        | :? int64 as x -> int32 x
                        | o -> failwith (sprintf "cannot convert %A to int32" o)
                        |> box))
                  typeof<int64>.GUID, 
                    (stdInj, 
                     fun r n -> getp r n (fun i ->
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
                let clearPrefix (pars: Dictionary<Guid, DbParameter>) prefix =
                    match paramsByPrefix.TryFind prefix with
                    | None -> ()
                    | Some p -> 
                        for i=0 to Array.length p - 1 do pars.[p.[i]].Value <- null
                match Map.tryFind (prefix + name) virtualDbFields with
                | Some project -> 
                    (fun (_: Dictionary<Guid,DbParameter>) (_: obj) -> ()), 
                    project prefix
                | None ->
                    match fld.PropertyType with
                    | typ when FSharpType.IsRecord(typ) ->
                        subRecord typ reader (prefix + fld.Name + "$")
                    | typ when Map.containsKey typ.GUID primitives ->
                        let (inj, proj) = primitives.[typ.GUID]
                        prim (fun pars p r -> inj pars p (reader r)) proj fld false
                    | typ when typ.GUID = typeof<Option<_>>.GUID 
                               && Map.containsKey typ.GenericTypeArguments.[0].GUID 
                                    primitives ->
                        let (inj, proj) = primitives.[typ.GenericTypeArguments.[0].GUID]
                        primOpt reader inj proj fld
                    | typ when typ.IsArray && typ.HasElementType
                               && Map.containsKey (typ.GetElementType()).GUID primitives ->
                        prim (fun _ p v -> p.Value <- reader v) 
                            (fun r n -> getp r n r.GetValue)
                            fld false
                    | typ when typ.GUID = typeof<Option<_>>.GUID
                               && let ityp = typ.GenericTypeArguments.[0]
                                  ityp.IsArray && ityp.HasElementType
                                  && Map.containsKey (ityp.GetElementType()).GUID primitives ->
                        primOpt reader (fun _ p v -> p.Value <- v) 
                            (fun r n -> getp r n r.GetValue) fld
                    | typ when (typ.GUID = typeof<Option<_>>.GUID
                                && FSharpType.IsRecord(typ.GenericTypeArguments.[0])) ->
                        option typ (fun none some get tag mkNone mkSome rtyp ->
                            let prefix = prefix + fld.Name + "$"
                            let (inject, project) = subRecord rtyp id prefix
                            let inject pars record =
                                let opt = reader record
                                let tag = tag opt
                                if tag = none.Tag then clearPrefix pars prefix
                                else if tag = some.Tag then inject pars (get opt)
                                else failwith "not supposed to get here!"
                            let project (r: DbDataReader) =
                                try mkSome [|project r|]
                                with _ -> mkNone [||]
                            (inject, project))
                    | typ when FSharpType.IsUnion(typ, true)
                               && typ.GenericTypeArguments = [||]
                               && (FSharpType.GetUnionCases(typ)
                                  |> Array.forall (fun c ->
                                       match c.GetFields () with
                                       | [||] -> true
                                       | [|ifo|] -> FSharpType.IsRecord(ifo.PropertyType)
                                       | _ -> false)) ->
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
                            FSharpType.GetUnionCases(typ) 
                            |> Array.map (fun c ->
                                c.Tag, (FSharpValue.PreComputeUnionConstructor(c),
                                        subRecord c))
                            |> Map.ofArray
                        prim 
                            (fun pars p v ->
                                let tag = tag (reader v)
                                p.Value <- tag
                                let (_mk, subrec) = Map.find tag mk
                                if subrecprefixes <> [] then 
                                    subrecprefixes |> List.iter (clearPrefix pars)
                                match subrec with
                                | None -> ()
                                | Some (inj, _proj) -> inj pars v)
                            (fun r n ->
                                let i = r.GetInt32(ord r n)
                                let (mk, subrec) = Map.find i mk
                                match subrec with
                                | None -> mk [||]
                                | Some (_, proj) -> mk [|proj r|]) 
                            fld false
                    | typ ->
                        let pk = FsPickler.GeneratePickler(typ)
                        let e = System.Text.Encoding.UTF8
                        prim 
                            (fun _ p v -> 
                                p.Value <- 
                                    e.GetString (jsonSer.PickleUntyped(reader v, pk)))
                            (fun r n -> 
                                let s = r.GetString(ord r n).Substring 1
                                jsonSer.UnPickleUntyped(e.GetBytes s, pk, encoding = e))
                            fld false)
            let construct = 
                FSharpValue.PreComputeRecordConstructor
                    (typ, allowAccessToPrivateRepresentation = true)
            let inject pars record = 
                injectAndProject |> Array.iter (fun (inj, _) -> inj pars record)
            let project r =
                let a = injectAndProject |> Array.map (fun (_, proj) -> proj r)
                construct a
            (inject, project)
        let (inject, project) = createInjectProject typ id prefix
        let columnNames = parameters.Values |> Seq.toArray
        let createParams create =
            let d = Dictionary<_,_>()
            parameters |> Seq.iter (fun kv -> d.[kv.Key] <- create kv.Value)
            d
        lens<'A>(createParams, columnNames,
            (fun pars t -> 
                inject pars (box t)
                vdf |> Map.iter (fun k (p, inject) -> inject pars.[p] t)),
            (fun r -> project r :?> 'A))

    static member Optional(lens: lens<'A>) : lens<Option<'A>> =
        let Inject (pars: Dictionary<Guid, DbParameter>) v =
            match v with
            | None -> pars |> Seq.iter (fun kv -> kv.Value.Value <- null)
            | Some v -> lens.Inject(pars, v)
        let Project(r: Data.Common.DbDataReader) = 
            let isNull col = r.IsDBNull (ord r col)
            if Array.forall isNull lens.ColumnNames then None
            else Some (lens.Project r)
        ofInjProj lens Inject Project
    static member Tuple(lensA: lens<'A>, lensB: lens<'B>, ?allowedIntersection) = 
        tuple lensA lensB (ai allowedIntersection)
    static member Tuple(lensA: lens<'A>, lensB: lens<'B>, lensC: lens<'C>,
                        ?allowedIntersection) =
        let t = tuple lensA lensB (ai allowedIntersection)
        let t = tuple t lensC (ai allowedIntersection)
        ofInjProj t (fun pars (a, b, c) -> t.Inject(pars, ((a, b), c)))
            (fun r -> let ((a, b), c) = t.Project r in (a, b, c))
    static member Tuple(lensA: lens<'A>, lensB: lens<'B>, 
                        lensC: lens<'C>, lensD: lens<'D>,
                        ?allowedIntersection) =
        let t = Lens.Tuple(lensA, lensB, lensC, ?allowedIntersection = allowedIntersection)
        let t = Lens.Tuple(t, lensD, ?allowedIntersection = allowedIntersection)
        ofInjProj t (fun pars (a, b, c, d) -> t.Inject(pars, ((a, b, c), d)))
            (fun r -> let ((a, b, c), d) = t.Project r in (a, b, c, d))
    static member Tuple(lensA: lens<'A>, lensB: lens<'B>, lensC: lens<'C>,
                        lensD: lens<'D>, lensE: lens<'E>,
                        ?allowedIntersection) =
        let t = 
            Lens.Tuple(lensA, lensB, lensC, lensD, 
                ?allowedIntersection = allowedIntersection)
        let t = Lens.Tuple(t, lensE, ?allowedIntersection = allowedIntersection)
        ofInjProj t (fun pars (a, b, c, d, e) -> t.Inject(pars, ((a, b, c, d), e)))
            (fun r -> let ((a, b, c, d), e) = t.Project r in (a, b, c, d, e))

type parameter<'A> internal (name: String) =
    member __.Name with get() = name

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

type query<'A, 'B> internal (sql:String, lens:lens<'B>, pars: String[],
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
        new query<_,_>(sql, lens, [|p1.Name|], fun p (p1: 'P1) -> p.[0].Value <- p1)
    static member Create(sql, lens, p1:parameter<'P1>, p2:parameter<'P2>) =
        new query<_,_>(sql, lens, [|p1.Name; p2.Name|],
            fun p (p1: 'P1, p2: 'P2) -> p.[0].Value <- p1; p.[1].Value <- p2)
    static member Create(sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
                         p3:parameter<'P3>) =
        new query<_, _>(sql, lens, [|p1.Name; p2.Name; p3.Name|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3) -> 
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, p3:parameter<'P3>, 
         p4:parameter<'P4>) =
        new query<_, _>(sql, lens, [|p1.Name;p2.Name;p3.Name;p4.Name|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4) -> 
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>) =
        new query<_, _>(sql, lens, [|p1.Name; p2.Name; p3.Name; p4.Name; p5.Name|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4, p5: 'P5) -> 
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4
                p.[4].Value <- p5)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>) =
        new query<_,_>(sql, lens, 
            [|p1.Name; p2.Name; p3.Name; p4.Name; p5.Name; p6.Name|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4, p5: 'P5, p6: 'P6) ->
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4
                p.[4].Value <- p5; p.[5].Value <- p6)
    static member Create
        (sql, lens, p1:parameter<'P1>, p2:parameter<'P2>, 
         p3:parameter<'P3>, p4:parameter<'P4>, p5:parameter<'P5>,
         p6:parameter<'P6>, p7:parameter<'P7>) =
        new query<_,_>(sql, lens, 
            [|p1.Name; p2.Name; p3.Name; p4.Name; p5.Name; p6.Name; p7.Name|],
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
            [|p1.Name; p2.Name; p3.Name; p4.Name; p5.Name; p6.Name; p7.Name;
              p8.Name|],
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
            [|p1.Name; p2.Name; p3.Name; p4.Name; p5.Name; p6.Name; p7.Name;
              p8.Name; p9.Name|],
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
            [|p1.Name; p2.Name; p3.Name; p4.Name; p5.Name; p6.Name; p7.Name;
              p8.Name; p9.Name; p10.Name|],
            fun p (p1: 'P1, p2: 'P2, p3: 'P3, p4: 'P4, 
                   p5: 'P5, p6: 'P6, p7: 'P7, p8: 'P8, p9: 'P9, p10: 'P10) ->
                p.[0].Value <- p1; p.[1].Value <- p2
                p.[2].Value <- p3; p.[3].Value <- p4
                p.[4].Value <- p5; p.[5].Value <- p6
                p.[6].Value <- p7; p.[7].Value <- p8
                p.[8].Value <- p9; p.[9].Value <- p10)

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
    abstract member Query: query<'A, 'B> * 'A -> Async<List<'B>>
    abstract member NonQuery: query<'A, NonQuery> * 'A -> Async<int>
    abstract member Insert: table:String * lens<'A> * seq<'A> -> Async<unit>
    abstract member Transaction: (asyncdb -> Async<'a>) -> Async<'a>
    abstract member NoRetry: (asyncdb -> Async<'A>) -> Async<'A>

type db =
    inherit IDisposable
    abstract member Query: query<'A, 'B> * 'A -> List<'B>
    abstract member NonQuery: query<'A, NonQuery> * 'A -> int
    abstract member Insert: table:String * lens<'A> * seq<'A> -> unit
    abstract member Transaction: (db -> 'a) -> 'a
    abstract member NoRetry: (db -> 'A) -> 'A

type Db internal () =
    static let getCmd (prepared: Dictionary<Guid, DbCommand>) 
        (con: #DbConnection) (provider: IProvider<_,_,_>) (q: query<_,_>)= 
        match prepared.TryFind q.Guid with
        | Some cmd -> cmd 
        | None ->
            let cmd = con.CreateCommand()
            let pars = 
                q.Parameters |> Array.map (fun name -> 
                    (provider.CreateParameter name :> DbParameter))
            cmd.Connection <- con
            cmd.CommandText <- q.Sql 
            cmd.Parameters.AddRange pars
            cmd.Prepare ()
            prepared.[q.Guid] <- cmd
            q.PushDispose (fun () -> ignore (prepared.Remove q.Guid))
            cmd
    static let buildTxn (txn: ref<Option<#DbTransaction>>) (provider: IProvider<_,_,_>) 
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
    static member Connect(provider: IProvider<_, _, 'TXN>) = 
        let con = provider.Connect ()
        let prepared = Dictionary<Guid, DbCommand>()
        let preparedInserts = 
            Dictionary<Guid, Dictionary<Guid, DbParameter>
                             * DbParameter[]
                             * (seq<obj[]> -> unit)>()
        let txn : ref<Option<'TXN>> = ref None
        let savepoints = Stack<String>()
        { new db with
            member __.Dispose() = con.Dispose()
            member db.NoRetry(f) = f db
            member __.NonQuery(q, a) = lock con (fun () -> 
                let cmd = getCmd prepared con provider q
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
                let cmd = getCmd prepared con provider q
                q.Set (cmd.Parameters, a)
                let r = cmd.ExecuteReader()
                read q.Lens r)
            member __.Insert(table, lens: lens<'A>, a: seq<'A>) = 
                let (pars, pararray, run) = 
                    match preparedInserts.TryFind lens.Guid with
                    | Some x -> x
                    | None ->
                        let pars = 
                            lens.CreateParams (fun name -> 
                                (provider.CreateParameter name :> DbParameter))
                        let pararray = pars |> Seq.map (fun kv -> kv.Value) |> Seq.toArray
                        let run = 
                            provider.InsertObject(con, table, 
                                pararray |> Array.map (fun p -> p.ParameterName))
                        preparedInserts.[lens.Guid] <- (pars, pararray, run)
                        (pars, pararray, run)
                let a = a |> Seq.map (fun a ->
                    lens.Inject(pars, a)
                    pararray |> Array.map (fun p -> p.Value))
                run a
            member db.Transaction(f) =
                let (rollback, commit) = buildTxn txn provider savepoints con
                try
                    let res = f db
                    commit ()
                    res
                with e -> rollback (); raise e }

    static member ConnectAsync(provider: IProvider<_,_,'TXN>) = async {
        let! con = provider.ConnectAsync ()
        let seq = new Sequencer()
        let prepared = Dictionary<Guid, DbCommand>()
        let preparedInserts = 
            Dictionary<Guid, Dictionary<Guid, DbParameter>
                             * DbParameter[]
                             * (seq<obj[]> -> Async<unit>)>()
        let txn : ref<Option<'TXN>> = ref None
        let savepoints = Stack<String>()
        return { new asyncdb with
            member __.Dispose() = 
                (seq :> IDisposable).Dispose()
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
            member __.Insert(table, lens: lens<'A>, a: seq<'A>) = seq.Enqueue(async {
                let (pars, pararray, run) = 
                    match preparedInserts.TryFind lens.Guid with
                    | Some x -> x
                    | None ->
                        let pars = 
                            lens.CreateParams (fun name -> 
                                (provider.CreateParameter name :> DbParameter))
                        let pararray = pars |> Seq.map (fun kv -> kv.Value) |> Seq.toArray
                        let run = 
                            provider.InsertObjectAsync(con, table, 
                                pararray |> Array.map (fun p -> p.ParameterName))
                        preparedInserts.[lens.Guid] <- (pars, pararray, run)
                        (pars, pararray, run)
                let a = a |> Seq.map (fun a ->
                    lens.Inject(pars, a)
                    pararray |> Array.map (fun p -> p.Value))
                return! run a })
            member db.Transaction(f) = async {
                let (rollback, commit) = buildTxn txn provider savepoints con
                try
                    let! res = f db
                    commit ()
                    return res
                with e -> rollback (); return raise e } } }

    static member WithRetries(provider: IProvider<_,_,_>, ?log, ?tries) = async {
        let log = defaultArg log ignore
        let tries = defaultArg tries 3
        let seq = new Sequencer()
        let mutable lastGoodDb : Option<asyncdb> = None
        let connect () = async {
            let! con = Db.ConnectAsync(provider)
            lastGoodDb <- Some con
            return con }
        let mutable con : Result<asyncdb, exn> = Error (Failure "not connected")
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
        return { new asyncdb with
            member __.NoRetry(f) = withDb (fun db -> async {
                safeToRetry <- false
                try return! f db
                finally safeToRetry <- true })
            member __.Dispose() = 
                (seq :> IDisposable).Dispose ()
                disposed <- true
                dispose () 
            member __.NonQuery(q, a) = withDb (fun db -> db.NonQuery(q, a))
            member __.Query(q, a) = withDb (fun db -> db.Query(q, a))
            member __.Insert(t, l, a) = withDb (fun db -> db.Insert(t, l, a))
            member __.Transaction(f) = withDb (fun db -> db.Transaction f) } }

