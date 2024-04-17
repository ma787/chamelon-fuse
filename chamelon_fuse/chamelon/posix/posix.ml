open Lwt.Infix
open Logs_syslog_lwt
open Mirage_lib

let root_pair = (0L, 1L)
let image = "./_build/default/chamelon_fuse/chamelon/src/image.img"
let block_size = 512

module Make(Sectors : Mirage_block.S)(Clock : Mirage_clock.PCLOCK) = struct
  module Fs = Fs.Make(Sectors)(Clock)
  module KV = Kv.Make(Sectors)(Clock)
  module Block_types = Block_type.Make(Sectors)

  type key = Mirage_kv.Key.t

  type t = Block_types.t

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

  let fs_connect _ = Sectors.connect image >>= fun block -> Fs.connect ~program_block_size:block_size ~block_size block >>= function
    | Ok t -> Lwt.return @@ t
    | Error _ -> Log.info (fun m -> m "failed to connect to %s" image); failwith (Printf.sprintf "failed to connect to %s" image)

  let get_fs_block fd = Block_types.This_Block.of_fd image fd >>= fun block ->
    Fs.connect ~program_block_size:block_size ~block_size (Block_types.This_Block.to_sectors block) >>= function
  | Ok t -> Lwt.return @@ t
  | Error _ -> Log.info (fun m -> m "failed to connect to %s" image); failwith (Printf.sprintf "failed to connect to %s" image)

  let get_kv_t _ = Sectors.connect image >>= fun block -> KV.connect ~program_block_size:block_size block >>= function
  | Ok t -> Lwt.return @@ t
  | Error _ -> Log.info (fun m -> m "failed to connect to %s" image); failwith (Printf.sprintf "failed to connect to %s" image)

  let get_kv_t_from_fd fd = Block_types.This_Block.of_fd image fd >>= fun block ->
    KV.connect ~program_block_size:block_size (Block_types.This_Block.to_sectors block) >>= function
    | Ok t -> Lwt.return @@ t
    | Error _ -> Log.info (fun m -> m "failed to connect to %s" image); failwith (Printf.sprintf "failed to connect to %s" image)

  let disconnect t = Block_types.This_Block.disconnect t.Block_types.block

  let mkdir path fd : (unit, Mirage_kv.write_error) result Lwt.t = get_fs_block fd >>= fun t ->
    let dir = Mirage_kv.Key.v path in
    Fs.mkdir t root_pair (Mirage_kv.Key.segments dir) >>= function
    | Error (`Not_found _) -> Lwt.return @@ (Error (`Not_found dir))
    | Error `No_space as e -> Lwt.return e
    | Ok _directory_head -> Lwt.return @@ Ok ()

  let rmdir path = get_kv_t () >>= fun t -> KV.remove t (Mirage_kv.Key.v path) >>= fun _ -> Lwt.return_unit

  let fopen (_path : string) =
    fs_connect () >>= fun t ->
    let fd, _, _, _, _, _ = Block_types.This_Block.get_elems t.Block_types.block in Lwt.return @@ fd

  let readdir path fd : ((string * [`Dictionary | `Value]) list, Mirage_kv.error) result Lwt.t = 
    get_kv_t_from_fd fd >>= fun t -> KV.list t (Mirage_kv.Key.v path) >>= function
    | Ok l -> Lwt.return @@ Ok l
    | Error _ -> Lwt.return @@ (Error (`Not_found (Mirage_kv.Key.v path)))

  let read path fd = get_kv_t_from_fd fd >>= fun t -> KV.get t (Mirage_kv.Key.v path) >>= function
  | Ok s -> Lwt.return @@ (s, 0)
  | Error _ -> Lwt.return @@ ("", -1)

  let write path fd s = 
    get_kv_t_from_fd fd >>= fun t -> KV.set t (Mirage_kv.Key.v path) s >>= function
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
    get_fs_block fd >>= fun t ->
    Block_types.This_Block.flush t.Block_types.block >>= function
    | Ok () -> Lwt.return @@ Ok ()
    | Error _ -> Lwt.return @@ Error `write_error
end
