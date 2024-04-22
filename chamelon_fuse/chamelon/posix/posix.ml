open Lwt.Infix
open Logs_syslog_lwt
open Mirage_lib

let root_pair = (0L, 1L)
let image = "/home/mohamed/Project/chamelon-fuse/chamelon_fuse/_build/default/chamelon_fuse/chamelon/src/image.img"
let block_size = 512
let program_block_size = 16

module Make(Sectors : Mirage_block.S)(Clock : Mirage_clock.PCLOCK) = struct
  module Fs = Fs.Make(Sectors)(Clock)
  module Block_types = Block_type.Make(Sectors)

  type key = Mirage_kv.Key.t

  type statvfs = {
    f_bsize : int64;
    f_frsize : int64;
    f_blocks : int64;
    f_bfree : int64;
    f_bavail : int64;
    f_files : int64;
    f_ffree : int64;
    f_favail : int64;
    f_fsid : int64;
    f_flag : int64;
    f_namemax : int64;
  }

  let install_logger () =
    unix_reporter () >|= function
      | Ok r -> Logs.set_reporter r
      | Error e -> print_endline e

  let _ = Lwt_main.run (install_logger ())

  let log_src = Lwt_main.run (Lwt.return (Logs.Src.create "chamelon-posix" ~doc:"chamelon POSIX layer"))
  module Log = (val Logs.src_log log_src : Logs.LOG)

  let fs = ref None

  let add_block block = match !fs with
  | None -> fs := Some block
  | Some _ -> fs := Some block

  let fd_match fd =
    match !fs with
    | None -> None
    | Some block ->
      let fopt, _, _, _, _, _ = Sectors.get_elems block in match fopt with
      | None -> None
      | Some f -> if fd = (Lwt_unix.unix_file_descr f) then Some block else None

  let connect block = Fs.connect ~program_block_size:program_block_size ~block_size block >>= function
    | Ok t -> Lwt.return @@ t
    | Error _ -> failwith "failed to connect to image"

  let disconnect block = Block_types.This_Block.disconnect block

  (* *)
  let connect_with_fd fd = match fd_match fd with
  | Some block -> connect block
  | None ->
    Sectors.connect image >>= fun block -> add_block block; connect block

  let fs_connect _ = match !fs with
    | None -> Sectors.connect image >>= fun block -> add_block block; connect block
    | Some block -> connect block

  module KV = struct
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

    let get = Fs.File_read.get

    let set t key data : (unit, write_error) result Lwt.t =
      let name_length = String.length @@ Mirage_kv.Key.basename key in
      if name_length > (Int32.to_int t.Block_types.name_length_max) then begin
        Log.err (fun f -> f "key length %d exceeds max length %ld - refusing to write" name_length t.Block_types.name_length_max);
        Lwt.return @@ Error (`Not_found Mirage_kv.Key.empty)
      end else begin
        let dir = Mirage_kv.Key.parent key in
        Fs.Find.find_first_blockpair_of_directory t root_pair
          (Mirage_kv.Key.segments dir) >>= function
        | `Basename_on block_pair ->
          Log.debug (fun m -> m "found basename of path %a on block pair %Ld, %Ld"
                        Mirage_kv.Key.pp key
                        (fst block_pair) (snd block_pair));
          (* the directory already exists, so just write the file *)
          Fs.File_write.set_in_directory block_pair t (Mirage_kv.Key.basename key) data
        | `No_id path -> begin
            Log.debug (fun m -> m "path component %s had no id; making it and its children" path);
            (* something along the path is missing, so make it. *)
            (* note we need to call mkdir with the whole path (except for the basename),
            * so that we get all levels of directory we may need,
            * not just the first thing that was found missing. *)
            Fs.mkdir t root_pair (Mirage_kv.Key.segments dir) >>= function
            | Error (`Not_found _) -> Lwt.return @@ (Error (`Not_found (Mirage_kv.Key.v path)))
            | Error `No_space as e -> Lwt.return e
            | Ok block_pair ->
              Log.debug (fun m -> m "made filesystem structure for %a, writing to blockpair %Ld, %Ld"
                            Mirage_kv.Key.pp dir (fst block_pair) (snd block_pair)
                        );
              Fs.File_write.set_in_directory block_pair t (Mirage_kv.Key.basename key) data
          end
        (* No_structs represents an inconsistent on-disk structure.
        * We can't do the right thing, so we return an error. *)
        | `No_structs ->
          Log.err (fun m -> m "id was present but no matching directory structure");
          Lwt.return @@ Error (`Not_found key)
      end

    (** [list t key], where [key] is a reachable directory,
    * gives the files and directories (values and dictionaries) in [key].
    * It is not a recursive listing. *)

    let list t key : ((string * [`Dictionary | `Value]) list, error) result Lwt.t =
      let cmp (name1, _) (name2, _) = String.compare name1 name2 in
      (* once we've found the (first) directory pair of the *parent* directory,
      * get the list of all entries naming files or directories
      * and sort them *)
      let ls_in_dir dir_pair =
        Fs.Find.all_entries_in_dir t dir_pair >>= function
        | Error _ -> Lwt.return @@ Error (`Not_found key)
        | Ok entries_by_block ->
          let translate entries = List.filter_map Chamelon.Entry.info_of_entry entries |> List.sort cmp in
          (* we have to compact first, because IDs are unique per *block*, not directory.
          * If we compact after flattening the list, we might wrongly conflate multiple
          * entries in the same directory, but on different blocks. *)
          let compacted = List.map (fun (_block, entries) -> Chamelon.Entry.compact entries) entries_by_block in
          Lwt.return @@ Ok (translate @@ List.flatten compacted)
      in
      (* find the parent directory of the [key] *)
      match (Mirage_kv.Key.segments key) with
      | [] -> ls_in_dir root_pair
      | segments ->
        (* descend into each segment until we run out, at which point we'll be in the
        * directory we want to list *)
        Fs.Find.find_first_blockpair_of_directory t root_pair segments >>= function
        | `No_id k ->
          (* be sure to return `k` as the error value, so the user might find out
          * which part of a complex path is missing and be more easily able to fix the problem *)
          Lwt.return @@ Error (`Not_found (Mirage_kv.Key.v k))
        (* No_structs is returned if part of the path is present, but not a directory (usually meaning
        * it's a file instead) *)
        | `No_structs -> Lwt.return @@ Error (`Not_found key)
        | `Basename_on pair -> ls_in_dir pair
    
    let remove t key =
      if Mirage_kv.Key.(equal empty key) then begin
        (* it's impossible to remove the root directory in littlefs, as it's
          * implicitly at the root pair *)
        Log.warn (fun m -> m "refusing to delete the root directory");
        Lwt.return @@ Error (`Not_found key)
      end else
        (* first, find the parent directory from which to delete (basename key) *)
        Fs.Find.find_first_blockpair_of_directory t root_pair Mirage_kv.Key.(segments @@ parent key) >>= function
        | `Basename_on pair ->
          Log.debug (fun f -> f "found %a in a directory starting at %a, will delete"
                        Mirage_kv.Key.pp key Fmt.(pair ~sep:comma int64 int64) 
                        pair);
          Fs.Delete.delete_in_directory pair t (Mirage_kv.Key.basename key)
        (* if we couldn't find (parent key), it's already pretty deleted *)
        | `No_id _ | `No_structs -> Lwt.return @@ Ok ()
  end

  let mkdir path (_mode : int) : (unit, Mirage_kv.write_error) result Lwt.t = fs_connect () >>= fun t ->
    let dir = Mirage_kv.Key.v path in
    Log.app (fun f -> f "calling Fs.mkdir");
    Fs.mkdir t root_pair (Mirage_kv.Key.segments dir) >>= function
    | Error (`Not_found _) -> Lwt.return @@ (Error (`Not_found dir))
    | Error `No_space as e -> Lwt.return e
    | Ok _directory_head -> Lwt.return @@ Ok ()

  let remove path = fs_connect () >>= fun t -> KV.remove t (Mirage_kv.Key.v path) >>= fun _ -> Lwt.return_unit

  let fopen (_path : string) =
    fs_connect () >>= fun t ->
    let fd, _, _, _, _, _ = Block_types.This_Block.get_elems t.Block_types.block in Lwt.return @@ fd

  let release (_path : string) = fs := None; Lwt.return_unit

  let readdir path fd : ((string * [`Dictionary | `Value]) list, Mirage_kv.error) result Lwt.t = 
    connect_with_fd fd >>= fun t -> KV.list t (Mirage_kv.Key.v path) >>= function
    | Ok l -> Lwt.return @@ Ok l
    | Error _ -> Lwt.return @@ (Error (`Not_found (Mirage_kv.Key.v path)))

  let read path fd = connect_with_fd fd >>= fun t -> KV.get t (Mirage_kv.Key.v path) >>= function
  | Ok s -> Lwt.return @@ (s, 0)
  | Error _ -> Lwt.return @@ ("", -1)

  let write path fd s = 
    connect_with_fd fd >>= fun t -> KV.set t (Mirage_kv.Key.v path) s >>= function
    | Ok () -> Lwt.return @@ Ok ()
    | Error `No_space as e -> Lwt.return e
    | Error _ -> Lwt.return @@ (Error (`Not_found (Mirage_kv.Key.v path)))

  let statfs (_path : string) = 
    fs_connect () >>= fun t ->
    let block_count = Int64.of_int (Block_types.This_Block.block_count t.Block_types.block) in
    let block_size = Int64.of_int t.Block_types.block_size in
    let unused = Int64.of_int (List.length !(t.Block_types.lookahead).blocks) in
    let free = Int64.sub block_count unused in
    let namemax = Int64.of_int32 t.Block_types.file_size_max in
    let stat =  {f_bsize = block_size;
                f_frsize = block_count;
                f_blocks = block_count;
                f_bfree = free;
                f_bavail = free;
                f_files = Int64.zero;
                f_ffree = Int64.zero;
                f_favail = Int64.zero;
                f_fsid = Int64.zero;
                f_flag = Int64.zero;
                f_namemax = namemax}
    in Lwt.return @@ stat

  let stat path =
    fs_connect () >>= fun t ->
    let err i = Lwt.return @@ Cstruct.create i in
    let info = Cstruct.create (1 + 4 + Int32.to_int (t.Block_types.name_length_max) + 1) in (* type : 8 bits, size : 32 bits, name : NAME_LENGTH_MAX+1 *)
    if (path = "/") then (* special case for root *)
      (Cstruct.set_uint8 info 0 0x002;
      Cstruct.blit_from_string "/" 0 info 5 1;
      Lwt.return @@ info)
  else
    Fs.Find.entries_of_name t root_pair path >>= function
    | Error _ -> err 1
    | Ok [] -> err 10
    | Ok compacted ->
      let entries = snd @@ List.(hd @@ rev compacted) in
      let name = List.find_opt (fun (tag, _data) ->
        Chamelon.Tag.((fst tag.type3 = LFS_TYPE_NAME))) in
      match name entries with
      | None -> err 2
      | Some (tag, data) ->
        let file_type = snd tag.type3 in
        Cstruct.set_uint8 info 0 file_type;
        Cstruct.blit data 0 info 5 (Cstruct.length data);
        if Int.equal file_type 0x002 then Lwt.return @@ info
        else
          (let inline_files = List.find_opt (fun (tag, _data) ->
            Chamelon.Tag.((fst tag.type3) = LFS_TYPE_STRUCT) &&
            Chamelon.Tag.((snd tag.type3) = 0x01)) in
          let ctz_files = List.find_opt (fun (tag, _block) ->
              Chamelon.Tag.((fst tag.type3 = LFS_TYPE_STRUCT) &&
                            Chamelon.Tag.((snd tag.type3 = 0x02)
                                        ))) in
          match inline_files entries, ctz_files entries with
          | None, None -> err 4
          | Some (tag, _data), None -> Cstruct.LE.set_uint32 info 1 (Int32.of_int tag.Chamelon.Tag.length); Lwt.return @@ info
          | _, Some (_tag, data) ->
            match Chamelon.File.ctz_of_cstruct data with
            | Some (_pointer, length) -> Cstruct.LE.set_uint32 info 1 (Int64.to_int32 length); Lwt.return @@ info
            | None -> err 7)

  let flush (_path : string) fd = 
    connect_with_fd fd >>= fun t ->
    Block_types.This_Block.flush t.Block_types.block >>= function
    | Ok () -> Lwt.return @@ Ok ()
    | Error _ -> Lwt.return @@ Error `write_error

  let mknod path mode =
    let fd = (Unix.openfile path [ O_CREAT; O_EXCL ] mode) in
    connect_with_fd fd >>= fun t ->
    KV.set t (Mirage_kv.Key.v path) "" >>= function
    | Ok () -> disconnect t.block >>= fun _ -> Lwt.return @@ Ok ()
    | Error _ -> disconnect t.block >>= fun _ -> Lwt.return @@ Error `write_error 
end