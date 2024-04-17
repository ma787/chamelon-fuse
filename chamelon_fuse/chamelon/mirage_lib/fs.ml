type blockpair = int64 * int64
type directory_head = blockpair

let root_pair = (0L, 1L)
let btree_root = 2L


let pp_blockpair = Fmt.(pair ~sep:comma int64 int64)

open Lwt.Infix

module Make(Sectors: Mirage_block.S)(Clock : Mirage_clock.PCLOCK) = struct
  module Fs_Btree = Btree.Make(Sectors)
  module Block_types = Block_type.Make(Sectors)

  open Block_types

  type write_result = [ `No_space | `Split | `Split_emergency ]

  type key = Mirage_kv.Key.t

  let log_src = Logs.Src.create "chamelon-fs" ~doc:"chamelon FS layer"
  module Log = (val Logs.src_log log_src : Logs.LOG)

  (* error type definitions straight outta mirage-kv *)
  type error = [
    | `Not_found           of key (** key not found *)
    | `Dictionary_expected of key (** key does not refer to a dictionary. *)
    | `Value_expected      of key (** key does not refer to a value. *)
  ]
  type write_error = [
    | error
    | `No_space                (** No space left on the device. *)
    | `Too_many_retries of int (** {!batch} has been trying to commit [n] times
                                   without success. *)
  ]

  let reverse_string s =
    let rec aux acc n lim =
      if n>=lim then acc
      else aux ((String.sub s n 1)::acc) (n+1) lim in
    let slen = String.length s in
    String.concat "" (aux [] 0 slen)
  
  module Read = struct

    (* get the wodge of data at this block number, and attempt to parse it *)
    let block_of_block_number {block_size; block; program_block_size; _} block_location =
      let cs = Cstruct.create block_size in
      This_Block.read block block_location [cs] >>= function
      | Error b -> Lwt.return @@ Error (`Block b)
      | Ok () ->
        match Chamelon.Block.of_cstruct ~program_block_size cs with
        | Error (`Msg s) ->
          Log.err (fun f -> f "error reading block %Ld : %s"
                      block_location s);
          Lwt.return @@ Error (`Chamelon `Corrupt)
        | Ok extant_block -> Lwt.return @@ Ok extant_block

    (* get the blocks at pair (first, second), parse them, and return whichever is more recent *)
    let block_of_block_pair t (l1, l2) =
      let open Lwt_result.Infix in
      Lwt_result.both (block_of_block_number t l1) (block_of_block_number t l2) >>= fun (b1, b2) ->
      if Chamelon.Block.(compare (revision_count b1) (revision_count b2)) < 0
      then Lwt.return @@ Ok b2
      else Lwt.return @@ Ok b1
          
  end

  module Traverse = struct   
    let build_btree t pointer bf =
      Fs_Btree.Serial.of_cstruct t bf pointer
    
    let all_btree_pointers tree = (Fs_Btree.Attrs.get_all_keys tree) @ (Fs_Btree.Ids.get_all_pointers [])

    let rec follow_links t visited = function
      | Chamelon.Entry.Data _ -> Lwt.return @@ Ok ([])
      | Chamelon.Entry.Metadata (a, b) ->
        match List.mem (a, b) visited with
        | true -> Log.err (fun f -> f "cycle detected: blockpair %a encountered again after initial visit." pp_blockpair (a, b));
          Lwt.return @@ Error `Disconnected
        | false ->
          Read.block_of_block_pair t (a, b) >>= function
          | Error (`Block e) ->
            Log.err (fun m -> m "error reading block pair %Ld, %Ld (0x%Lx, 0x%Lx): %a"
                        a b a b This_Block.pp_error e);
            Lwt.return @@ Error e
          | Error (`Chamelon `Corrupt) ->
            Log.err (fun f -> f "filesystem seems corrupted; we couldn't make sense of a block pair");
            Lwt.return @@ Error `Disconnected
          | Ok block ->
            Log.debug (fun f -> f "finding blocks linked from %a" pp_blockpair (a, b));
            let links = Chamelon.Block.linked_blocks block in
            let string_of_link_list l = List.fold_left (fun acc lk -> match lk with
            | Chamelon.Entry.Metadata (a, b) -> acc ^ "metadata: (" ^ (Int64.to_string a) ^ ", " ^ (Int64.to_string b) ^ "), "
            | Chamelon.Entry.Data (a, b) -> acc ^ "data: (" ^ (Int64.to_string a) ^ ", " ^ (Int64.to_string b) ^ "), ") "" l in
            Log.debug (fun f -> f "found %d linked blocks: %s" (List.length links) (string_of_link_list links));
            Lwt_list.fold_left_s (fun so_far link ->
                match so_far with
                | Error _ as e -> Lwt.return e
                | Ok l ->
                  follow_links t ((a, b)::visited) link >>= function
                  | Error e ->
                    Log.err (fun f -> f "filesystem seems corrupted; we couldn't get a list of unused blocks: %a" This_Block.pp_error e);
                    Lwt.return @@ Error `Disconnected
                  | Ok new_links -> Lwt.return @@ Ok (new_links @ l)
              ) (Ok []) links
            >>= function
            | Ok list -> Lwt.return @@ Ok (a :: b :: list)
            | e -> Lwt.return e
        
    let follow_links_tree t visited pair tree =
      follow_links t visited pair >>= function
      | Error _ as e -> Lwt.return e
      | Ok (used_blocks) -> Lwt.return @@ Ok (used_blocks @ (all_btree_pointers tree))

    (* [last_block t pair] returns the last blockpair in the hardtail
     * linked list starting at [pair], which may well be [pair] itself *)
    let rec last_block t pair =
      let open Lwt_result.Infix in
      Read.block_of_block_pair t pair >>= fun block ->
      match List.find_opt (fun e ->
          Chamelon.Tag.is_hardtail (fst e)
        ) (Chamelon.Block.entries block) with
      | None -> Lwt.return @@ Ok pair
      | Some entry -> match Chamelon.Dir.hard_tail_links entry with
        | None -> Lwt.return @@ Ok pair
        | Some next_pair -> last_block t next_pair
  end

  module Allocate = struct

    let unused t used_blocks =
      let module IntSet = Set.Make(Int64) in
      let actual_blocks = This_Block.block_count t.block in
      let pool = IntSet.of_list @@ List.init actual_blocks Int64.of_int in
      0, IntSet.(elements @@ diff pool (of_list used_blocks))

    let populate_lookahead ~except t tree =
      Traverse.follow_links_tree t [] (Chamelon.Entry.Metadata root_pair) tree >|= function
      | Error e ->
        Log.err (fun f -> f "error attempting to find unused blocks: %a" This_Block.pp_error e);
        Error `No_space
      | Ok used_blocks ->
        Log.debug (fun f -> f "%d blocks used: %a" (List.length used_blocks) Fmt.(list ~sep:sp uint64) used_blocks);
        Ok (unused t (used_blocks @ except))

    let add_to_lookahead t new_blocks = 
      let used = !(t.lookahead).blocks in
      t.lookahead := {!(t.lookahead) with blocks=(used @ new_blocks)}

    let get_blocks t n tree: (int64 list, write_error) result Lwt.t =
      let recover l =
        let open Lwt_result.Infix in
        populate_lookahead ~except:l t tree >>= function
        | _, [] -> 
          Log.err (fun f -> f "number of b-tree file and head/child block pointers: %d" (List.length (Traverse.all_btree_pointers tree)));
          Log.err (fun f -> f "no blocks remain free on filesystem");
          Lwt.return @@ Error `No_space
        | off, new_l -> 
          t.lookahead := ({offset = off; blocks = new_l});
          Lwt.return @@ Ok () in
      let get_block t =
        match !(t.lookahead).blocks with
        | p::ps ->
          t.lookahead := {!(t.lookahead) with blocks = ps};
          Lwt.return @@ Ok p
        | [] -> Lwt.return @@ Error `No_space
      in
      if n <= 0 then Lwt.return @@ Ok []
      else let rec aux t acc n =
        if n <= 0 then Lwt.return @@ Ok acc
        else match !(t.lookahead).blocks with
        | l when List.length l = n ->
          t.lookahead := {!(t.lookahead) with blocks = []};
          Lwt.return @@ Ok (l @ acc)
        | l when List.length l > n -> 
          (get_block t >>= function
          | Error _ -> recover acc >>= (function
            | Error _ as e -> Lwt.return e
            | Ok () -> aux t acc n)
          | Ok b -> aux t (b::acc) (n-1))
        | l ->
          let lnl = List.length l in
          if lnl = 0 then recover acc >>= (function
            | Error _ as e -> Lwt.return e
            | Ok () -> aux t acc n)
          else
            (t.lookahead := {!(t.lookahead) with blocks = []};
            aux t (l @ acc) (n-lnl))
      in aux t [] n

    (* [get_block_pair fs] wraps [get_blocks fs 2] to return a pair for the caller's convenience *)
    let get_block_pair t tree =
      get_blocks t 2 tree >|= function
      | Ok (block1::block2::_) -> Ok (block1, block2)
      | Ok _ | Error _ -> Error `No_space

  end

  module Fs_data : sig
    val btree : Btree.node ref
    val update_btree : Btree.node -> unit
    val get_file_table : int64 -> int64 list
    val add_file : int64 -> int64 list -> unit
    val remove_file : t -> int64 -> (unit, [> `Not_found]) result Lwt.t
  end = struct
    let btree = ref (Btree.Lf ([], [], false, 0, -1))
    let fat = ref []

    let update_btree tree = btree := tree

    let get_file_table pointer = List.assoc pointer !fat

    let add_file pointer pointers =
      let current = !fat in
      let newl = (pointer, pointers)::current in
      fat := newl

    let remove_file t pointer = 
      try 
      let plist = List.assoc pointer !fat in
      Fs_Btree.delete_list t !btree plist [] >>= (function
      | Error _ -> Lwt.return @@ Error `Not_found
      | Ok (newt, pointers) ->
        let newl = List.filter (fun (f, _) -> not (Int64.equal f pointer)) !fat in
        btree := newt; fat := newl; Allocate.add_to_lookahead t pointers;
        Lwt.return @@ Ok ())
      with Not_found -> Lwt.return @@ Error `Not_found
    end

  module Write = struct
    (* from the littlefs spec, we should be checking whether
     * the on-disk data matches what we have in memory after
     * doing this write. Then if it doesn't, we should rewrite
     * to a different block, and note the block as bad so we don't
     * try to write to it in the future.
     *
     * I don't think that's necessary in our execution context.
     * we're not writing directly to a flash controller,
     * we're probably writing to a file on another filesystem
     * managed by an OS with its own bad block detection.
     * That's my excuse for punting on it for now, anyway. *)
    let block_to_block_number t data block_location : (unit, write_result) result Lwt.t =
      let block_device = t.block in
      This_Block.write block_device block_location [data] >>= function
      | Ok () -> Lwt.return @@ Ok ()
      | Error e ->
        Log.err (fun m -> m "block write error: %a" This_Block.pp_write_error e);
        Lwt.return @@ Error `No_space

    let rec block_to_block_pair t data (b1, b2) : (unit, write_result) result Lwt.t =
      let split data  =
        Allocate.get_block_pair t !Fs_data.btree >>= function
        | Error `No_space -> Lwt.return @@ Error `No_space
        | Ok (new_block_1, new_block_2) when Int64.equal new_block_1 new_block_2 ->
          (* if there is only 1 block remaining, we'll get the same one twice.
           * That's not enough for the split. *)
          Lwt.return @@ Error `No_space
        | Ok (new_block_1, new_block_2) -> begin
            Log.debug (fun m -> m "splitting block pair %Ld, %Ld to %Ld, %Ld"
                           b1 b2 new_block_1 new_block_2);
            let old_block, new_block = Chamelon.Block.split data (new_block_1, new_block_2) in
            Log.debug (fun m -> m "keeping %d entries in the old block, and putting %d in the new one"
                           (List.length @@ Chamelon.Block.entries old_block)
                           (List.length @@ Chamelon.Block.entries new_block));
            (* be very explicit about writing the new block first, and only overwriting
             * the old block pair if the new block pair write succeeded *)
            block_to_block_pair t new_block (new_block_1, new_block_2) >>= function
            | Error `Split | Error `Split_emergency | Error `No_space ->
              Lwt.return @@ Error `No_space
            | Ok () -> begin
                Log.debug (fun f -> f "wrote new pair; overwriting old pair");
                (* ignore any warnings about needing to split, etc *)
                let cs1 = Cstruct.create t.block_size in
                let serialize = Chamelon.Block.into_cstruct ~program_block_size:t.program_block_size in
                let _ = serialize cs1 old_block in
                Lwt_result.both
                  (block_to_block_number t cs1 b1)
                  (block_to_block_number t cs1 b2) >>= function
                | Ok _ -> Lwt.return @@ Ok ()
                | Error _ as e -> Lwt.return e
              end
          end
      in
      let cs1 = Cstruct.create t.block_size in
      let serialize = Chamelon.Block.into_cstruct ~program_block_size:t.program_block_size in
      match serialize cs1 data with
      | `Ok -> begin
        Lwt_result.both
          (block_to_block_number t cs1 b1)
          (block_to_block_number t cs1 b2)
        >>= function
        | Ok _ -> Lwt.return @@ Ok ()
        | Error _ as e -> Lwt.return e
      end
      | `Split | `Split_emergency | `Unwriteable ->
        (* try a compaction first *)
        Cstruct.memset cs1 0x00;
        let compacted = Chamelon.Block.compact data in
        Log.debug (fun m -> m "split requested for a block with %d entries. compacted, we had %d"
                       (List.length @@ Chamelon.Block.entries data)
                       (List.length @@ Chamelon.Block.entries compacted)
                   );
        match serialize cs1 compacted, Chamelon.Block.hardtail compacted with
        | `Ok, _ -> begin
            Log.debug (fun f -> f "compaction was sufficient. Will not split %a" pp_blockpair (b1, b2));
            Lwt_result.both
              (block_to_block_number t cs1 b1)
              (block_to_block_number t cs1 b2)
            >>= function
            | Ok _ -> Lwt.return @@ Ok ()
            | Error _ as e -> Lwt.return e
          end
        | `Split, None | `Split_emergency, None -> begin
            Log.debug (fun f -> f "compaction was insufficient and the block has no hardtail. Splitting %a" pp_blockpair (b1, b2));
            split compacted
          end
        | `Split, _ -> begin
            Log.debug (fun f -> f "split needed, but the block's already split. Writing the compacted block");
            Lwt_result.both
              (block_to_block_number t cs1 b1)
              (block_to_block_number t cs1 b2)
            >>= function
            | Ok _ -> Lwt.return @@ Ok ()
            | Error _ as e -> Lwt.return e
          end
        | `Split_emergency, _  | `Unwriteable, _ ->
          Log.err (fun f -> f "Couldn't write to block %a" pp_blockpair (b1, b2));
          Lwt.return @@ Error `No_space
  end

  module Find : sig
    type blockwise_entry_list = blockpair * (Chamelon.Entry.t list)

    (** [all_entries_in_dir t head] gives an *uncompacted* list of all
     * entries in the directory starting at [head].
     * the returned entries in the directory are split up by block,
     * so the caller can distinguish between re-uses of the same ID number
     * when the directory spans multiple block numbers *)
    val all_entries_in_dir : t -> directory_head ->
      (blockwise_entry_list list, error) result Lwt.t

    (** [entries_of_name t head name] scans [head] (and any subsequent blockpairs in the directory's
     * hardtail list) for `id` entries matching [name]. If an `id` is found for [name],
     * all entries matching `id` from the directory are returned (compacted). *)
    val entries_of_name : t -> directory_head -> string -> (blockwise_entry_list list,
                                                           [`No_id of key
                                                           | `Not_found of key]
                                                          ) result Lwt.t

    (** [find_first_blockpair_of_directory t head l] finds and enters
     *  the segments in [l] recursively until none remain.
     *  It returns `No_id if an entry is not present and `No_structs if an entry
     *  is present, but does not represent a valid directory. *)
    val find_first_blockpair_of_directory : t -> directory_head -> string list ->
      [`Basename_on of directory_head | `No_id of string | `No_structs] Lwt.t

  end = struct
    type blockwise_entry_list = blockpair * (Chamelon.Entry.t list)

    (* nb: all does mean *all* here; the list is returned uncompacted,
     * so the caller may have to compact to avoid reporting on expired state *)
    let rec all_entries_in_dir t block_pair =
      Read.block_of_block_pair t block_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v "hard_tail"))
      | Ok block ->
        let this_blocks_entries = Chamelon.Block.entries block in
        match Chamelon.Block.hardtail block with
        | None -> Lwt.return @@ Ok [(block_pair, this_blocks_entries)]
        | Some nextpair ->
          all_entries_in_dir t nextpair >>= function
          | Ok entries -> Lwt.return @@ Ok ((block_pair, this_blocks_entries) :: entries)
          | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v "hard_tail"))

    let entries_of_name t block_pair name =
      let entries_of_id entries id =
        let matches (tag, _) = 0 = compare tag.Chamelon.Tag.id id in
        List.find_all matches entries
      in
      let id_of_key entries key =
        let data_matches c = 0 = String.(compare key @@ Cstruct.to_string c) in
        let tag_matches t = Chamelon.Tag.(fst t.type3 = LFS_TYPE_NAME)
        in
        match List.find_opt (fun (tag, data) ->
            tag_matches tag && data_matches data
          ) entries with
        | Some (tag, _) -> Some tag.Chamelon.Tag.id
        | None -> None
      in
      let open Lwt_result in
      all_entries_in_dir t block_pair >>= fun entries_by_block ->
      let entries_matching_name (block, entries) =
        match id_of_key (Chamelon.Entry.compact entries) name with
        | None ->
          Log.debug (fun m -> m "id for %S not found in %d entries from %a"
                         name (List.length entries) pp_blockpair block);
          Error (`No_id (Mirage_kv.Key.v name))
        | Some id ->
          Log.debug (fun m -> m "name %S is associated with id %d on blockpair %a"
                         name id pp_blockpair block);
          let entries = entries_of_id entries id in
          Log.debug (fun m -> m "found %d entries for id %d in %a"
                         (List.length entries) id pp_blockpair block);
          let compacted = Chamelon.Entry.compact entries in
          Log.debug (fun m -> m "after compaction, there were %d entries for id %d in %a"
                         (List.length compacted) id pp_blockpair block);
          Ok (block, compacted)
      in
      let blockwise_matches = List.filter_map (fun es ->
          match entries_matching_name es with
          | Ok (_, []) | Error _ -> None
          | Ok (block, l) -> Some (block, l)
        ) entries_by_block in
      Lwt.return (Ok blockwise_matches)

    let rec find_first_blockpair_of_directory t block_pair key =
      match key with
      | [] -> Lwt.return @@ `Basename_on block_pair
      | key::remaining ->
        entries_of_name t block_pair key >>= function
        | Error _ -> Lwt.return @@ `No_id key
        | Ok [] -> Lwt.return @@ `No_id key
        | Ok l ->
          (* just look at the last entry with this name, and get the entries *)
          let l = snd @@ List.(hd @@ rev l) in
          match List.filter_map Chamelon.Dir.of_entry l with
          | [] -> Lwt.return `No_structs
          | next_blocks::_ ->
            find_first_blockpair_of_directory t next_blocks remaining

  end

  let last_modified_value t key =
    (* find (parent key) on the filesystem *)
    Find.find_first_blockpair_of_directory t root_pair Mirage_kv.Key.(segments @@ parent key) >>= function
    | `No_structs -> Lwt.return @@ Error (`Not_found key)
    | `No_id k -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
    | `Basename_on block_pair ->
      (* get all the entries in (parent key) *)
      Find.entries_of_name t block_pair @@ Mirage_kv.Key.basename key >>= function
      | Error (`No_id k) | Error (`Not_found k) -> Lwt.return @@ Error (`Not_found k)
      | Ok l ->
	(* [l] contains all entries associated with (basename key),
	 * some of which are hopefully last-updated metadata entries. *)
	(* We don't care which block any of the entries were on *)
	let l = List.(map snd l |> flatten) in
	(* find the mtime-looking entries *)
	match List.find_opt (fun (tag, _data) ->
	    Chamelon.Tag.(fst @@ tag.type3) = LFS_TYPE_USERATTR &&
	    Chamelon.Tag.(snd @@ tag.type3) = 0x74
	  ) l with
	| None ->
	  Log.warn (fun m -> m "Key %a found but it had no time attributes associated" Mirage_kv.Key.pp key);
	  Lwt.return @@ Error (`Not_found key)
	| Some (_tag, data) ->
	  match Chamelon.Entry.ctime_of_cstruct data with
	  | None ->
	    Log.err (fun m -> m "Time attributes (%a) found for %a but they were not parseable" Cstruct.hexdump_pp data Mirage_kv.Key.pp key);

	    Lwt.return @@ Error (`Not_found key)
	  | Some k -> Lwt.return @@ Ok k


  let rec mkdir t parent_blockpair key =
    (* mkdir has its own function for traversing the directory structure
     * because we want to make anything that's missing along the way,
     * rather than having to get an error, mkdir, get another error, mkdir... *)
    let follow_directory_pointers t block_pair = function
      | [] -> begin
        Traverse.last_block t block_pair >>= function
        | Ok pair -> Lwt.return @@ `New_writes_to pair
        | Error _ ->
          Log.err (fun f -> f "error finding last block from blockpair %Ld, %Ld"
                      (fst block_pair) (snd block_pair));
          Lwt.return @@ `New_writes_to block_pair
      end
      | key::remaining ->
        Find.entries_of_name t block_pair key >>= function
        | Error _ -> Lwt.return @@ `Not_found key
        | Ok [] -> Lwt.return `No_structs
        | Ok l ->
          (* we only care about the last block with entries, and don't care
           * which block it is *)
          let l = snd @@ List.(hd @@ rev l) in
          match List.filter_map Chamelon.Dir.of_entry l with
          | [] -> Lwt.return `No_structs
          | next_blocks::_ -> Lwt.return (`Continue (remaining, next_blocks))
    in 
    (* [find_or_mkdir t parent_blockpair dirname] will:
     * attempt to find [dirname] in the directory starting at [parent_blockpair] or any other blockpairs in its hardtail linked list
     * if no valid entries corresponding to [dirname] are found:
        * allocate new blocks for [dirname]
        * find the last blockpair (q, r) in [parent_blockpair]'s hardtail linked list
        * create a directory entry for [dirname] in (q, r)
     *)
    let find_or_mkdir t parent_blockpair (dirname : string) =
      follow_directory_pointers t parent_blockpair [dirname] >>= function
      | `Continue (_path, next_blocks) -> Lwt.return @@ Ok next_blocks
      | `New_writes_to next_blocks -> Lwt.return @@ Ok next_blocks
      | `No_structs | `Not_found _ ->
        Lwt_mutex.with_lock t.new_block_mutex @@ fun () ->
        (* for any error case, try making the directory *)
        (* TODO: it's probably wise to put a delete entry first here if we got No_structs
         * or another "something weird happened" case *)
        (* first off, get a block pair for our new directory *)
        Allocate.get_block_pair t !Fs_data.btree >>= function
        | Error _ -> Lwt.return @@ Error (`No_space)
        | Ok (dir_block_0, dir_block_1) ->
          (* first, write empty commits to the new blockpair; if that fails,
           * we want to bail before making any more structure *)
          Write.block_to_block_pair t (Chamelon.Block.of_entries ~revision_count:1 []) (dir_block_0, dir_block_1) >>= function
          | Error _ ->
            Log.err (fun f -> f "error initializing a directory at a fresh block pair (%Ld, %Ld)"
                         dir_block_0 dir_block_1);
            Lwt.return @@ Error `No_space
          | Ok () ->
            (* we want to write the entry for our new subdirectory to
             * the *last* blockpair in the parent directory, so follow
             * all the hardtails *)
            Traverse.last_block t parent_blockpair >>= function
            | Error _ -> Lwt.return @@ Error (`Not_found dirname)
            | Ok last_pair_in_dir ->
              Log.debug (fun f -> f "found last pair %a in directory starting at %a"
                             pp_blockpair last_pair_in_dir
                             pp_blockpair parent_blockpair);
              Read.block_of_block_pair t last_pair_in_dir >>= function
              | Error _ -> Lwt.return @@ Error (`Not_found dirname)
              | Ok block_to_write ->
                let extant_ids = Chamelon.Block.ids block_to_write in
                let dir_id = match Chamelon.Block.IdSet.max_elt_opt extant_ids with
                  | None -> 1
                  | Some s -> s + 1
                in
                let name = Chamelon.Dir.name dirname dir_id in
                let dirstruct = Chamelon.Dir.mkdir ~to_pair:(dir_block_0, dir_block_1) dir_id in
                let new_block = Chamelon.Block.add_commit block_to_write [name; dirstruct] in
                Write.block_to_block_pair t new_block last_pair_in_dir >>= function
                | Error _ -> Lwt.return @@ Error `No_space
                | Ok () -> Lwt.return @@ Ok (dir_block_0, dir_block_1)
    in
    match key with
    | [] -> Lwt.return @@ Ok parent_blockpair
    | dirname::more ->
      let open Lwt_result in
      (* finally, the recursive bit: when we've successfully found or made a directory
       * but we have more components in the path,
       * find or make the next component relative to the juts-returned blockpair *)
      find_or_mkdir t parent_blockpair dirname >>= fun newpair ->
      mkdir t newpair more

  module File_read : sig
    val get : t -> Mirage_kv.Key.t -> (string, error) result Lwt.t
    val get_value : t -> (int64 * int64) -> string -> ([> `Ctz of int64 * int | `Inline of string],[> `Not_found of string | `Value_expected of string]) result Lwt.t
    val get_partial : t -> Mirage_kv.Key.t -> offset:int -> length:int ->
       (string, error) result Lwt.t

  end = struct
    let get_btree_file t key (pointer, file_size) =
      let rec read_file pointer acc read =
        Fs_Btree.Serial.read_data_block t pointer >>= (function
          | Error _ as e -> Lwt.return e
          | Ok cs ->
            let p, data_region = Chamelon.File.of_block cs in
            let n_read = read + Cstruct.length data_region in
            if n_read >= file_size then Lwt.return @@ Ok (data_region::acc)
            else read_file p (data_region::acc) n_read) in
      read_file pointer [] 0 >>= (function
        | Error _ -> Lwt.return @@ Error (`Not_found key)
        | Ok (data) ->
          let cs = Cstruct.sub (Cstruct.concat data) 0 file_size in
          Lwt.return @@ Ok (Cstruct.to_string cs))

    let get_value t parent_dir_head filename =
      Find.entries_of_name t parent_dir_head filename >|= function
      | Error _ | Ok [] -> Error (`Not_found filename)
      | Ok compacted ->
        (* if there are >1 block with entries, we only care about the last one *)
        let compacted = snd @@ List.(hd @@ rev compacted) in
        let inline_files = List.find_opt (fun (tag, _data) ->
            Chamelon.Tag.((fst tag.type3) = LFS_TYPE_STRUCT) &&
            Chamelon.Tag.((snd tag.type3) = 0x01)
          )
        in
        let ctz_files = List.find_opt (fun (tag, _block) ->
            Chamelon.Tag.((fst tag.type3 = LFS_TYPE_STRUCT) &&
                          Chamelon.Tag.((snd tag.type3 = 0x02)
                                       ))) in
        Log.debug (fun m -> m "found %d entries with name %s" (List.length compacted) filename);
        match inline_files compacted, ctz_files compacted with
        | Some (_tag, data), None -> Ok (`Inline (Cstruct.to_string data))
        | None, None -> begin
          (* is it actually a directory? *)
            match List.find_opt (fun (tag, _data) ->
                Chamelon.Tag.((fst tag.type3) = LFS_TYPE_STRUCT) &&
                Chamelon.Tag.((snd tag.type3) = 0x00)
              ) compacted with
            | Some _ -> Error (`Value_expected filename)
            | None ->
              Log.debug (fun f -> f "an ID was found for the filename, but no entries are structs or files");
              Error (`Not_found filename)
        end
        | _, Some (_, ctz) ->
          match Chamelon.File.ctz_of_cstruct ctz with
          | Some (pointer, length) -> Ok (`Ctz (pointer, Int64.to_int length))
          | None -> Error (`Value_expected filename)

    let get t key : (string, error) result Lwt.t =
      let map_result = function
        | Ok (`Inline d) -> Lwt.return (Ok d)
        | Ok (`Ctz ctz) -> get_btree_file t key ctz
        | Error (`Not_found k) -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
        | Error (`Value_expected k) -> Lwt.return @@ Error (`Value_expected (Mirage_kv.Key.v k))
      in
      match Mirage_kv.Key.segments key with
      | [] -> Lwt.return @@ Error (`Value_expected key)
      | basename::[] -> get_value t root_pair basename >>= map_result
      | _ ->
        let dirname = Mirage_kv.Key.(parent key |> segments) in
        Find.find_first_blockpair_of_directory t root_pair dirname >>= function
        | `Basename_on pair -> begin
            get_value t pair (Mirage_kv.Key.basename key) >>= map_result
          end
        | _ -> Lwt.return @@ Error (`Not_found key)

    let get_btree_partial t key ~offset ~length (pointer, file_size) =
      let b_size = t.block_size - Fs_Btree.sizeof_pointer in
      let rec get_index byte i =
      if byte <= b_size then i
      else get_index (byte-b_size) (i-1) in
      let rec read_file pointers index starti acc =
        Fs_Btree.Serial.read_data_block t (List.nth pointers index) >>= (function
          | Error _ as e -> Lwt.return e
          | Ok cs ->
            let _, data_region = Chamelon.File.of_block cs in
            if index = starti then Lwt.return @@ Ok (data_region::acc)
            else read_file pointers (index+1) starti (data_region::acc)) in
      let table = pointer::(Fs_data.get_file_table pointer) in
      let tlen = List.length table in
      let last_index = tlen - 1 in
      assert (Int.equal 0 (get_index file_size last_index));
      assert (Int.equal last_index (get_index 0 last_index));
      let start_index, end_index = get_index offset last_index, get_index (offset+length) last_index in
      read_file table end_index start_index [] >>= (function
        | Error _ -> Lwt.return @@ Error (`Not_found key)
        | Ok [] -> Lwt.return @@ Ok ""
        | Ok (hd::blocks) ->
          let hd_off = Chamelon.File.first_byte_on_index ~block_size:t.block_size start_index last_index in
          let first_block_offset = offset - hd_off in
          let new_hd = Cstruct.shift hd first_block_offset in
          let offset_cs = Cstruct.concat @@ new_hd :: blocks |> Cstruct.to_string in       
          let flength = if (offset+length) > file_size then (file_size-offset) else length in
          let s = String.sub offset_cs 0 flength in
          Lwt.return @@ Ok s)

    let get_partial t key ~offset ~length : (string, error) result Lwt.t =
      if offset < 0 then begin
        Log.err (fun f -> f "read requested with negative offset");
        Lwt.return @@ Error (`Not_found key)
      end else if length <= 0 then begin
        Log.err (fun f -> f "read requested with length <= 0");
        Lwt.return @@ Error (`Not_found key)
      end else begin
        let map_result = function
          | Error (`Not_found k) -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
          | Error (`Value_expected k) -> Lwt.return @@ Error (`Value_expected (Mirage_kv.Key.v k))
          | Ok (`Inline d) -> begin
            try Lwt.return @@ Ok (String.sub d offset @@ min length @@ (String.length d) - offset)
            with Invalid_argument _ -> Lwt.return @@ Error (`Not_found key)
          end
          | Ok (`Ctz ctz) ->
            get_btree_partial t key ~offset ~length ctz
        in
        match Mirage_kv.Key.segments key with
        | [] -> Lwt.return @@ Error (`Value_expected key)
        | basename::[] -> get_value t root_pair basename >>= map_result
        | _ ->
          let dirname = Mirage_kv.Key.(parent key |> segments) in
          Find.find_first_blockpair_of_directory t root_pair dirname >>= function
          | `Basename_on pair -> begin
              get_value t pair (Mirage_kv.Key.basename key) >>= map_result
            end
          | _ -> Lwt.return @@ Error (`Not_found key)
      end
  end

  module Size = struct

    let get_file_size t parent_dir_head filename =
      Find.entries_of_name t parent_dir_head filename >|= function
      | Error _ | Ok [] -> Error (`Not_found (Mirage_kv.Key.v filename))
      | Ok compacted ->
        let entries = snd @@ List.(hd @@ rev compacted) in
        let inline_files = List.find_opt (fun (tag, _data) ->
            Chamelon.Tag.((fst tag.type3) = LFS_TYPE_STRUCT) &&
            Chamelon.Tag.((snd tag.type3) = 0x01)
          )
        in
        let ctz_files = List.find_opt (fun (tag, _block) ->
            Chamelon.Tag.((fst tag.type3 = LFS_TYPE_STRUCT) &&
                          Chamelon.Tag.((snd tag.type3 = 0x02)
                                       ))) in
        Log.debug (fun m -> m "found %d entries with name %s" (List.length compacted) filename);
        match inline_files entries, ctz_files entries with
        | None, None -> Error (`Not_found (Mirage_kv.Key.v filename))
        | Some (tag, _data), None ->
          Ok tag.Chamelon.Tag.length
        | _, Some (_tag, data) ->
          match Chamelon.File.ctz_of_cstruct data with
          | Some (_pointer, length) -> Ok (Int64.to_int length)
          | None -> Error (`Value_expected (Mirage_kv.Key.v filename))

    let rec size_all t blockpair =
      Find.all_entries_in_dir t blockpair >>= function
      | Error _ -> Lwt.return 0
      | Ok l ->
        let entries = List.(map snd l |> flatten) in
        Lwt_list.fold_left_s (fun acc e ->
            match Chamelon.Content.size e with
            | `File n -> Lwt.return @@ acc + n
            | `Skip -> Lwt.return @@ acc
            | `Dir p ->
              Log.debug (fun f -> f "descending into dirpair %a" pp_blockpair p);
              size_all t p >>= fun s -> Lwt.return @@ s + acc
          ) 0 entries

    let size t key : (int, error) result Lwt.t =
      Log.debug (fun f -> f "getting size on key %a" Mirage_kv.Key.pp key);
      match Mirage_kv.Key.segments key with
      | [] -> size_all t root_pair >>= fun i -> Lwt.return @@ Ok i
      | basename::[] -> get_file_size t root_pair basename
      | segments ->
        Log.debug (fun f -> f "descending into segments %a" Fmt.(list ~sep:comma string) segments);
        Find.find_first_blockpair_of_directory t root_pair segments >>= function
        | `Basename_on p -> size_all t p >|= fun i -> Ok i
        | `No_id _ | `No_structs -> begin
            (* no directory by that name, so try for a file *)
            Find.find_first_blockpair_of_directory t root_pair segments >>= function
            | `Basename_on pair -> begin
                get_file_size t pair (Mirage_kv.Key.basename key)
              end
            | _ -> Lwt.return @@ Error (`Not_found key)
        end

  end

  module File_write : sig
    (** [set_in_directory directory_head t filename data] creates entries in
     * [directory] for [filename] pointing to [data] *)
    val set_in_directory : directory_head -> t -> string -> string ->
      (unit, write_error) result Lwt.t

  end = struct
    let rec write_btree_block t file_pointers written so_far data tree =
      if Int.compare so_far (String.length data) >= 0 then begin
        Lwt.return @@ Ok (tree, written)
      end else
          let pointer = List.hd file_pointers in
          let b_size = t.block_size - Fs_Btree.sizeof_pointer in
          let to_write = min b_size (String.length data - so_far) in
          let pl = String.sub data so_far to_write in
          let next = try List.hd written with Failure _ -> Int64.max_int in
          Fs_Btree.insert_and_write t tree pointer pl next >>= (function
          | Error _ -> 
            Log.err (fun f -> f "get_blocks gave us too few blocks for our file");
            Lwt.return @@ Error `No_space
          | Ok tr -> write_btree_block t (List.tl file_pointers) (pointer::written) (so_far + to_write) data tr)

    let write_btree t data =
      let b_size = t.block_size - Fs_Btree.sizeof_pointer in
      let n_blocks = Int.of_float (Float.ceil (Float.of_int (String.length data) /. Float.of_int b_size )) in
      Log.debug (fun f -> f "taking %d blocks from lookahead" n_blocks);
      Allocate.get_blocks t n_blocks !Fs_data.btree >>= function
        | Error _ as e -> Lwt.return e
        | Ok fpointers -> write_btree_block t fpointers [] 0 data !Fs_data.btree

    (* Find the correct directory structure in which to write the metadata entry for the CTZ pointer.
     * Write the CTZ, then write the metadata. *)
    let rec write_in_ctz dir_block_pair t filename data entries =
      Read.block_of_block_pair t dir_block_pair >>= function
      | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
      | Ok root ->
        match Chamelon.Block.hardtail root with
        | Some next_blockpair -> write_in_ctz next_blockpair t filename data entries
        | None ->
          let file_size = String.length data in
          write_btree t data >>= function
          | Error _ as e -> Lwt.return e
          | Ok (_, []) -> Lwt.return @@ Error `No_space
          | Ok (tree, last_pointer::pointers) ->
            (* the file has been written; find an ID and write the appropriate metadata *)
            Fs_data.update_btree tree;
            Fs_data.add_file last_pointer pointers;
            let next = match Chamelon.Block.(IdSet.max_elt_opt @@ ids root) with
              | None -> 1
              | Some n -> n + 1
            in
            let name = Chamelon.File.name filename next in
            let ctime = Chamelon.Entry.ctime next (Clock.now_d_ps ()) in
            let ctz = Chamelon.File.create_ctz next
                ~pointer:last_pointer ~file_size:(Int64.of_int file_size)
            in
            let new_entries = entries @ [name; ctime; ctz] in
            Log.debug (fun m -> m "writing %d entries for ctz for file %s of size %d" (List.length new_entries) filename file_size);
            let new_block = Chamelon.Block.add_commit root new_entries in
            Write.block_to_block_pair t new_block dir_block_pair >>= function
            | Error `No_space -> Lwt.return @@ Error `No_space
            | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
            | Ok () -> Lwt.return @@ Ok ()

    let rec write_inline block_pair t filename data entries =
      Read.block_of_block_pair t block_pair >>= function
      | Error _ ->
        Log.err (fun m -> m "error reading block pair %Ld, %Ld"
                     (fst block_pair) (snd block_pair));
        Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v filename))
      | Ok extant_block ->
        match Chamelon.Block.hardtail extant_block with
        | Some next_blockpair -> write_inline next_blockpair t filename data entries
        | None ->
          let used_ids = Chamelon.Block.ids extant_block in
          let next = match Chamelon.Block.IdSet.max_elt_opt used_ids with
            | None -> 1
            | Some n -> n + 1
          in
          let ctime = Chamelon.Entry.ctime next (Clock.now_d_ps ()) in
          let file = Chamelon.File.write_inline filename next (Cstruct.of_string data) in
          let commit = entries @ (ctime :: file) in
          Log.debug (fun m -> m "writing %d entries for inline file %s" (List.length file) filename);
          let new_block = Chamelon.Block.add_commit extant_block commit in
          Write.block_to_block_pair t new_block block_pair >>= function
          | Error `No_space -> Lwt.return @@ Error `No_space
          | Error `Split | Error `Split_emergency ->
            Log.err (fun m -> m "couldn't write a block, because it got too big");
            Lwt.return @@ Error `No_space
          | Ok () -> Lwt.return @@ Ok ()

    let set_in_directory block_pair t (filename : string) data =
      if String.length filename < 1 then Lwt.return @@ Error (`Value_expected Mirage_kv.Key.empty)
      else begin
        Lwt_mutex.with_lock t.new_block_mutex @@ fun () ->
        Find.entries_of_name t block_pair filename >>= function
        | Error (`Not_found _ ) as e -> Lwt.return e
        | Ok [] | Ok ((_, [])::_) | Error (`No_id _) -> begin
            Log.debug (fun m -> m "writing new file %s, size %d" filename (String.length data));
            if (String.length data) > (t.block_size / 4) then
              write_in_ctz block_pair t filename data []
            else
              write_inline block_pair t filename data []
          end
        | Ok ((block, hd::_tl)::_) ->
          (* we *could* replace the previous ctz/inline entry,
           * instead of deleting the whole mapping and replacing it,
           * but since we do both the deletion and the new addition
           * in the same commit, I think this saves us some potentially
           * error-prone work *)
          let id = Chamelon.Tag.((fst hd).id) in
          Log.debug (fun m -> m "deleting existing entry %s at id %d" filename id);
          let delete = (Chamelon.Tag.(delete id), Cstruct.create 0) in
          (* we need to make sure our deletion and new items go in the same block pair as
           * the originals did *)
          if (String.length data) > (t.block_size / 4) then
            write_in_ctz block t filename data [delete]
          else
            write_inline block t filename data [delete]
        (* TODO: we should probably handle separately the case where multiple
         * blocks have entries matching the name in question -- currently we
         * will only handle the first such block (that remains after compaction) *)
      end

  end

  module Delete = struct
    let delete_file_data t dir filename = 
      File_read.get_value t dir filename >>= function
      | Ok (`Inline _) -> Lwt.return @@ Ok ()
      | Ok (`Ctz ctz) -> (match ctz with
        | pointer, _file_size -> Fs_data.remove_file t pointer)
      | Error (`Not_found _) -> Lwt.return @@ Ok ()
      | Error (`Value_expected _) -> Lwt.return @@ Ok ()

    let delete_in_directory directory_head t name =
      Find.entries_of_name t directory_head name >>= function
        (* several "it's not here" cases *)
      | Error (`No_id _) | Error (`Not_found _) ->
        Log.debug (fun m -> m "no id or nothing found for %s" name);
        Lwt.return @@ Ok ()
      | Ok [] | Ok ((_,[])::_) ->
        Log.debug (fun m -> m "no entries on %a for %s"
                       pp_blockpair directory_head name);
        Lwt.return @@ Ok ()
      | Ok ((blockpair_with_id, hd::_tl)::_) ->
        delete_file_data t blockpair_with_id name >>= (function
        | Error _ -> 
          Log.debug (fun m -> m "file not found in dir for %s" name);
          Lwt.return @@ Ok ()
        | Ok () ->
          let id = Chamelon.Tag.((fst hd).id) in
          Log.debug (fun m -> m "id %d found for name %s on block %a" id name
                   pp_blockpair blockpair_with_id);
          Read.block_of_block_pair t blockpair_with_id >>= function
          | Error _ -> Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v name))
          | Ok block ->
            Log.debug (fun m -> m "adding deletion for id %d on block pair %a"
                         id pp_blockpair blockpair_with_id);
            let deletion = Chamelon.Tag.delete id in
            let new_block = Chamelon.Block.add_commit block [(deletion, Cstruct.empty)] in
            Write.block_to_block_pair t new_block blockpair_with_id >>= function
            | Error _ -> Lwt.return @@ Error `No_space
            | Ok () -> Lwt.return @@ Ok ())

  end

(* [block_size_device] is the block size used by the underlying block device which
 * we're trying to mount a filesystem from,
 * as opposed to the block size recorded in the filesystem's superblock *)
  let check_superblock ~program_block_size ~block_size_device cs =
    match Chamelon.Block.of_cstruct ~program_block_size cs with
    | Error (`Msg s) ->
      Log.err (fun f -> f "error parsing block when checking superblock: %s" s);
      Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
    | Ok parsed_block ->
      (* does this block have the expected superblock entries? *)
      (* the order of entries is strictly specified: name, then the inline struct, then
       * any other entries in the superblock *)
      match Chamelon.Block.entries parsed_block with
      | maybe_name :: maybe_struct :: _ when
          Chamelon.Superblock.is_valid_name maybe_name &&
          Chamelon.Superblock.is_valid_superblock maybe_struct -> begin
        match Chamelon.Superblock.parse (snd maybe_struct) with
        | Error (`Msg s) ->
          Log.err (fun f -> f "error parsing block when checking superblock: %s" s);
          Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
        | Ok sb ->
          if sb.version_major != fst Chamelon.Superblock.version then begin
            Log.err (fun f -> f "filesystem is an incompatible version");
            Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
          end else if not @@ Int32.equal sb.block_size block_size_device then begin
            Log.err (fun f -> f "filesystem expects a block device with size %ld but the device block size is %ld" sb.block_size block_size_device);
            Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
          end else Lwt.return @@ Ok (Chamelon.Block.revision_count parsed_block, sb)
      end
      | _ -> Log.err (fun f -> f "expected entries not found on parsed superblock");
        Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)

  (* `device` should be an already-connected block device *)
  let connect ~program_block_size ~block_size device : (t, error) result Lwt.t =
    let block = This_Block.to_t device in
    let block0, block1= Cstruct.create block_size, Cstruct.create block_size in
    Lwt_result.both 
      (This_Block.read block 0L [block0])
      (This_Block.read block 1L [block1])
    >>= function
    | Error e ->
      Log.err (fun f -> f "first blockpair read failed: %a" This_Block.pp_error e);
      Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
    | Ok ((), ()) ->
      (* make sure the block is parseable and block size matches *)
      Lwt_result.both
        (check_superblock ~program_block_size ~block_size_device:(Int32.of_int block_size) block0)
        (check_superblock ~program_block_size ~block_size_device:(Int32.of_int block_size) block1)
      >>= function
      | Error _ as e -> Lwt.return e
      | Ok ((rc0, sb0), (rc1, sb1)) ->
        let lookahead = ref {offset = 0; blocks = []} in
        let file_size_max, name_length_max =
          if rc1 > rc0 then sb1.file_size_max, sb1.name_length_max else sb0.file_size_max, sb0.name_length_max
        in
        let t = {block;
                 block_size;
                 program_block_size;
                 lookahead;
                 file_size_max;
                 name_length_max;
                 new_block_mutex = Lwt_mutex.create ()}
        in
        Log.debug (fun f -> f "mounting fs with file size max %ld, name length max %ld" file_size_max name_length_max);
        Lwt_mutex.lock t.new_block_mutex >>= fun () ->
        let bf = (t.block_size + 1 - 3) / (2*Fs_Btree.sizeof_pointer) in
        Traverse.build_btree t btree_root bf >>= function
        | Error _e ->
          Lwt_mutex.unlock t.new_block_mutex;
          Log.err (fun f -> f "couldn't build b-tree");
          Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
        | Ok btree ->
          (Traverse.follow_links_tree t [] (Chamelon.Entry.Metadata root_pair) btree >>= function
          | Error _e ->
            Lwt_mutex.unlock t.new_block_mutex;
            Log.err (fun f -> f "couldn't get list of used blocks");
            Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
          | Ok used_blocks ->
            Log.debug (fun f -> f "found %d used blocks on block-based key-value store: %a" (List.length used_blocks) Fmt.(list ~sep:sp int64) used_blocks);
            Fs_data.update_btree btree;
            let open Allocate in
            let offset, blocks = unused t used_blocks in
            let lookahead = ref {offset; blocks} in
            Lwt_mutex.unlock t.new_block_mutex;
            Lwt.return @@ Ok {t with lookahead; block; block_size; program_block_size})

  let format ~program_block_size ~block_size (sectors : Sectors.t) :
    (unit, write_error) result Lwt.t =
    let block = This_Block.to_t sectors in
    let write_whole_block n b = This_Block.write block n
        [fst @@ Chamelon.Block.to_cstruct ~program_block_size ~block_size b]
    in
    let name = Chamelon.Superblock.name in
    let block_count = This_Block.block_count block in
    let superblock_inline_struct = Chamelon.Superblock.inline_struct (Int32.of_int block_size) (Int32.of_int block_count) in
    let block_0 = Chamelon.Block.of_entries ~revision_count:1 [name; superblock_inline_struct] in
    let block_1 = Chamelon.Block.of_entries ~revision_count:2 [name; superblock_inline_struct] in
    Lwt_result.both
    (write_whole_block (fst root_pair) block_0)
    (write_whole_block (snd root_pair) block_1) >>= function
    | Ok ((), ()) -> Lwt.return @@ Ok ()
    | _ -> Lwt.return @@ Error `No_space

end
