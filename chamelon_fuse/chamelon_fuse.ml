open Fuse
open Lwt.Infix
open Logs_syslog_lwt
open Unix

module Littlefs = Posix.Make(Block)(Pclock)

let install_logger () =
  unix_reporter () >|= function
    | Ok r -> Logs.set_reporter r
    | Error e -> print_endline e

let _ = Lwt_main.run (install_logger ())
let log_src = Lwt_main.run (Lwt.return (Logs.Src.create "chamelon-fuse" ~doc:"chamelon FUSE driver"))
module Log = (val Logs.src_log log_src : Logs.LOG)

let flag_to_string flag = let open Unix in match flag with
| O_APPEND -> "O_APPEND"
| O_CLOEXEC -> "O_CLOEXEC"
| O_CREAT -> "O_CREAT"
| O_DSYNC -> "O_DSYNC"
| O_EXCL -> "O_EXCL"
| O_KEEPEXEC -> "O_KEEPEXEC"
| O_NOCTTY -> "O_NOCTTY"
| O_NONBLOCK -> "O_NONBLOCK"
| O_RDONLY -> "O_RDONLY"
| O_RDWR -> "O_RDWR"
| O_RSYNC -> "O_RSYNC"
| O_SHARE_DELETE -> "O_SHARE_DELETE"
| O_SYNC -> "O_SYNC"
| O_TRUNC -> "O_TRUNC"
| O_WRONLY -> "O_WRONLY"

let flag_list_to_string flags = List.fold_left (fun acc s -> acc ^ " " ^ (flag_to_string s)) "" flags

let u_stat = Lwt_main.run @@ (Lwt.return @@ LargeFile.stat ".")
let u_statfs = Lwt_main.run @@ (Lwt.return @@ Unix_util.statvfs ".")

let chamelon_statfs path =
  let open Unix_util in
  Lwt_main.run @@ (
      Log.app (fun f -> f "statfs call, path = %s" path);
      Littlefs.statfs path >>= fun stat ->
      let fs_stat = {f_bsize = stat.f_bsize;
      f_frsize = stat.f_frsize;
      f_blocks = stat.f_blocks;
      f_bfree = stat.f_bfree;
      f_bavail = stat.f_bavail;
      f_files = u_statfs.f_files;
      f_ffree = u_statfs.f_ffree;
      f_favail = u_statfs.f_favail;
      f_fsid = u_statfs.f_fsid;
      f_flag = u_statfs.f_flag;
      f_namemax = stat.f_namemax}
      in Lwt.return @@ fs_stat)

let chamelon_stat path =
  let open Unix.LargeFile in
  Lwt_main.run @@ (
    Log.app (fun f -> f "stat call, path = %s" path);
    Littlefs.stat path >>= fun info ->
      try
        let file_type, st_size, name = Cstruct.get_uint8 info 0, Cstruct.LE.get_uint32 info 1, Cstruct.to_string info ~off:5 in
        Log.app (fun f -> f "file name: %s" name);
        let kind, size = 
          if Int.equal file_type 0x002 then "S_DIR", u_stat.st_size
          else "S_REG", (Int64.of_int32 st_size)in
        Log.app (fun f -> f "got kind %s and size %Ld from chamelon stat call, path = %s" kind size path);
        Lwt.return @@ {st_dev = 0;
        st_ino = u_stat.st_ino;
        st_kind = (if Int.equal file_type 0x002 then S_DIR else S_REG);
        st_perm = if Int.equal file_type 0x001 then 0o444 else u_stat.st_perm;
        st_nlink = 1;
        st_uid = u_stat.st_uid;
        st_gid = u_stat.st_gid;
        st_rdev = u_stat.st_rdev;
        st_size = (if Int.equal file_type 0x001 then Int64.of_int32 (Cstruct.LE.get_uint32 info 1) else u_stat.st_size);
        st_atime = u_stat.st_atime;
        st_mtime = u_stat.st_mtime;
        st_ctime = u_stat.st_ctime}
      with Invalid_argument _ -> raise (Unix_error (ENOENT, "stat", path))
  )

let chamelon_readdir path (_fd : int) =
  Lwt_main.run @@ (
    Log.app (fun f -> f "readdir call, path = %s" path);
      Littlefs.readdir path >>= function
      | Error _ -> Log.app (fun f -> f "error reading entries from path %s" path); Lwt.return @@ []
      | Ok entries -> Log.app (fun f -> f "successfully read entries from path %s" path); Lwt.return @@ (List.map fst entries)
  )

let do_fopen path flags = Log.app (fun f -> f "fopen, path = %s, flags = [ %s]" path (flag_list_to_string flags)); None

let chamelon_read path (buf : buffer) (_offset : int64) (_fd : int) =
  Lwt_main.run @@ (
    Log.app (fun f -> f "read call, path = %s" path);
    Littlefs.read path >>= function
    | ("", -1) -> Log.app (fun f -> f "error reading %s" path); Lwt.return @@ -1
    | (v, _) -> 
      let len = String.length v in
      let buf_cs = Cstruct.of_bigarray buf in
      Cstruct.blit_from_string v 0 buf_cs 0 len; (* write to memory buffer *)
      Log.app (fun f -> f "successfully read %d bytes from %s into memory buffer" (String.length v) path);
      Lwt.return @@ len)

let chamelon_write path (buf : buffer) offset fd =
  Lwt_main.run @@ (
    Log.app (fun f -> f "write call, path = %s, fd = %d, offset = %Ld" path fd offset);
    let buf_cs = Cstruct.of_bigarray buf ~off:(Int64.to_int offset) in
    let data = Cstruct.to_string buf_cs in
    let to_write, err =
      if String.equal data "-" then begin
        match Bos.OS.File.(read dash) with
        | Error _ -> "", -1
        | Ok data -> data, 0
      end else data, 0 in
    if Int.equal err (-1) then (Log.app (fun m -> m "couldn't understand what I should write\n%!"); Lwt.return @@ -1)
    else 
      (Littlefs.write path to_write >>= (function
      | Ok () -> Log.app (fun f -> f "successfully wrote %d bytes to %s, fd = %d" (String.length to_write) path fd); Lwt.return @@ (String.length to_write)
      | Error _ -> Log.app (fun f -> f "error when writing to %s, fd = %d" path fd); Lwt.return @@ -1))
 )

let _ =
  main Sys.argv
    {
      default_operations with
      getattr = chamelon_stat;
      statfs = chamelon_statfs;
      readdir = chamelon_readdir;
      fopen = do_fopen;
      read = chamelon_read;
      write = chamelon_write;
      mkdir = (fun path mode -> Lwt_main.run @@ (Littlefs.mkdir path mode >>= fun _ -> Lwt.return_unit));
      rmdir = (fun path -> Lwt_main.run @@ (Littlefs.remove path));
      unlink = (fun path -> Lwt_main.run @@ (Littlefs.remove path));
      mknod = (fun path _mode -> Lwt_main.run @@ (Littlefs.mknod path));
      utime = (fun _path _atime _mtime -> ());
    }
