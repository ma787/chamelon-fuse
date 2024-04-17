open Fuse
open Lwt.Infix
open Logs_syslog_lwt

module Littlefs = Posix.Make(Block)(Pclock)

let install_logger () =
  unix_reporter () >|= function
    | Ok r -> Logs.set_reporter r
    | Error e -> print_endline e

let _ = Lwt_main.run (install_logger ())
let log_src = Lwt_main.run (Lwt.return (Logs.Src.create "chamelon-fuse" ~doc:"chamelon FUSE driver"))
module Log = (val Logs.src_log log_src : Logs.LOG)

let retrieve_descr i = Unix_util.file_descr_of_int i
let store_descr fd = Unix_util.int_of_file_descr fd

let chamelon_read path (buf : buffer) (_offset : int64) fd =
  Lwt_main.run @@ (
    Littlefs.read path (retrieve_descr fd) >>= function
    | ("", -1) -> Lwt.return @@ -1
    | (v, _) -> 
      let buf_cs = Cstruct.of_bigarray buf in
      Cstruct.blit_from_string v 0 buf_cs 0 (String.length v); (* write to memory buffer *)
      Lwt.return @@ 0)

let chamelon_write path (buf : buffer) (_offset : int64) fd =
  Lwt_main.run @@ (
    Log.app (fun f -> f "writing file %s" path);
    let buf_cs = Cstruct.of_bigarray buf in
    let data = Cstruct.to_string buf_cs in
    let errno = ref 0 in
    let to_write =
      if String.equal data "-" then begin
        match Bos.OS.File.(read dash) with
        | Error _ -> Logs.err (fun m -> m "couldn't understand what I should write\n%!"); errno := 1; ""
        | Ok data -> data
      end else data
    in
    if (!errno = 1) then Lwt.return @@ -1
    else 
      (Littlefs.write path (retrieve_descr fd) to_write >>= (function
      | Ok () -> Lwt.return @@ 0
      | Error _ -> Lwt.return @@ -1))
  )

let chamelon_remove path = Lwt_main.run @@ (Littlefs.rmdir path)

let chamelon_mkdir path fd = Lwt_main.run @@ (Littlefs.mkdir path (retrieve_descr fd) >>= function
| Ok () -> Log.app (fun f -> f "directory %s created successfully" path) ; Lwt.return_unit
| Error _ -> Log.app (fun f -> f "error while creating directory %s" path); Lwt.return_unit)

let chamelon_statfs path =
  let open Unix_util in
  Lwt_main.run @@ (
      let u_stat = statvfs path in
      Littlefs.statfs path >>= fun stat ->
      let fs_stat = {f_bsize = stat.f_bsize;
      f_frsize = stat.f_frsize;
      f_blocks = stat.f_blocks;
      f_bfree = stat.f_bfree;
      f_bavail = stat.f_bavail;
      f_files = u_stat.f_files;
      f_ffree = u_stat.f_ffree;
      f_favail = u_stat.f_favail;
      f_fsid = u_stat.f_fsid;
      f_flag = u_stat.f_flag;
      f_namemax = stat.f_namemax}
      in Lwt.return @@ fs_stat)

let chamelon_stat path =
  let open Unix.LargeFile in
  Lwt_main.run @@ (
    Lwt_unix.stat path >>= fun u_stat ->
      Log.app (fun f -> f "got the following info from unix stat call (path = %s):
    ino : %d, perm: %d, nlink: %d, uid: %d, gid: %d, rdev: %d, size: %d,
    atime: %f, mtime: %f, ctime: %f" path u_stat.st_ino u_stat.st_perm u_stat.st_nlink u_stat.st_uid u_stat.st_gid u_stat.st_rdev
    u_stat.st_size u_stat.st_atime u_stat.st_mtime u_stat.st_ctime);
      Littlefs.stat path >>= fun info ->
        Log.app (fun f -> f "got cstruct of length %d from chamelon stat call, path = %s" (Cstruct.length info) path);
        let file_type = Cstruct.get_uint8 info 0 in
        Lwt.return @@ {st_dev = 0;
        st_ino = u_stat.st_ino;
        st_kind = (if Int.equal file_type 0x002 then S_DIR else S_REG);
        st_perm = u_stat.st_perm;
        st_nlink = u_stat.st_nlink;
        st_uid = u_stat.st_uid;
        st_gid = u_stat.st_gid;
        st_rdev = u_stat.st_rdev;
        st_size = (if Int.equal file_type 0x001 then Int64.of_int32 (Cstruct.LE.get_uint32 info 1) else Int64.of_int u_stat.st_size);
        st_atime = u_stat.st_atime;
        st_mtime = u_stat.st_mtime;
        st_ctime = u_stat.st_ctime})

let chamelon_fsync (_path : string) (_ds : bool) fd =
  Lwt_main.run @@ (Lwt_unix.fsync (Lwt_unix.of_unix_file_descr (retrieve_descr fd)))

let chamelon_fopen path (_flags : Unix.open_flag list) =
  Lwt_main.run @@ (
    Littlefs.fopen path >>= function
    | Some fd -> Log.app (fun f -> f "fopen got fd %d" (store_descr (Lwt_unix.unix_file_descr fd))); Lwt.return @@ Some (store_descr (Lwt_unix.unix_file_descr fd))
    | None -> Lwt.return @@ None
  )

let chamelon_release (_path : string) (_mode : Unix.open_flag list) fd =
  Lwt_main.run @@ (Log.app (fun f -> f "closing fd %d" fd); Lwt_unix.close (Lwt_unix.of_unix_file_descr (retrieve_descr fd)))

let chamelon_flush path fd = Lwt_main.run @@ (Littlefs.flush path (retrieve_descr fd) >>= fun _ -> Lwt.return_unit)

let chamelon_readdir path fd =
  Lwt_main.run @@ (
      Littlefs.readdir path (retrieve_descr fd) >>= function
      | Error _ -> Lwt.return @@ []
      | Ok entries -> Lwt.return @@ (List.map fst entries)
  )

let _ =
  main Sys.argv
    {    
      init = (fun _ -> Lwt_main.run @@ (Lwt.return_unit));
      statfs = chamelon_statfs;

      getattr = chamelon_stat;

      mkdir = chamelon_mkdir;
      rmdir = chamelon_remove;
      opendir = chamelon_fopen;
      releasedir = chamelon_release;
      readdir = chamelon_readdir;

      unlink = chamelon_remove;
      
      fopen = chamelon_fopen;
      release = chamelon_release;
      read = chamelon_read;
      write = chamelon_write;
      fsync = chamelon_fsync;
      flush = chamelon_flush;

      fsyncdir = chamelon_fsync;

      (* unsupported operations *)
      truncate = Fuse_lib.undefined;
      rename = Fuse_lib.undefined;
      symlink = Fuse_lib.undefined;
      readlink = Fuse_lib.undefined;
      listxattr = Fuse_lib.undefined;
      getxattr = Fuse_lib.undefined;
      setxattr = Fuse_lib.undefined;
      removexattr = Fuse_lib.undefined;
      link = Fuse_lib.undefined;
      mknod = Fuse_lib.undefined;
      chmod = Fuse_lib.undefined;
      chown = Fuse_lib.undefined;
      utime = Fuse_lib.undefined;
    }
