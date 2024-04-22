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

let retrieve_descr i = Unix_util.file_descr_of_int i
let store_descr fd = Unix_util.int_of_file_descr fd

let chamelon_read path (buf : buffer) (_offset : int64) fd =
  Lwt_main.run @@ (
    Log.app (fun f -> f "read call, path = %s, fd = %d" path fd);
    Littlefs.read path (retrieve_descr fd) >>= function
    | ("", -1) -> Log.app (fun f -> f "error reading %s, fd = %d" path fd); Lwt.return @@ -1
    | (v, _) -> 
      let buf_cs = Cstruct.of_bigarray buf in
      Cstruct.blit_from_string v 0 buf_cs 0 (String.length v); (* write to memory buffer *)
      Log.app (fun f -> f "successfully read %d bytes from %s into memory buffer, fd = %d" (String.length v) path fd);
      Lwt.return @@ 0)

let chamelon_write path (buf : buffer) (_offset : int64) fd =
  Lwt_main.run @@ (
    Log.app (fun f -> f "write call, path = %s, fd = %d" path fd);
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
      | Ok () -> Log.app (fun f -> f "successfully wrote %d bytes to %s, fd = %d" (String.length to_write) path fd); Lwt.return @@ 0
      | Error _ -> Log.app (fun f -> f "error when writing to %s, fd = %d" path fd); Lwt.return @@ -1))
  )

let chamelon_unlink path = Lwt_main.run @@ (Log.app (fun f -> f "unlink call, path = %s" path); Lwt_unix.unlink path >>= fun _ -> Littlefs.remove path)

let chamelon_rmdir path = Lwt_main.run @@ (Log.app (fun f -> f "rmdir call, path = %s" path); Lwt_unix.rmdir path >>= fun _ -> Littlefs.remove path)

let chamelon_mkdir path mode = 
  Lwt_main.run @@ (
    Log.app (fun f -> f "mkdir call, path = %s, mode = %d" path mode);
    Lwt_unix.mkdir path mode >>= fun _ ->
    Littlefs.mkdir path mode >>= fun _ -> Lwt.return_unit)

let chamelon_statfs path =
  let open Unix_util in
  Lwt_main.run @@ (
      Log.app (fun f -> f "statfs call, path = %s" path);
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
    Log.app (fun f -> f "stat call, path = %s" path);
    Lwt_unix.stat path >>= fun u_stat ->
      Littlefs.stat path >>= fun info ->
        try
          let file_type = Cstruct.get_uint8 info 0 in
          let kind, size = 
            if Int.equal file_type 0x002 then "S_DIR", (Int64.of_int u_stat.st_size) 
            else "S_REG", (Int64.of_int32 (Cstruct.LE.get_uint32 info 1)) in
          Log.app (fun f -> f "got kind %s and size %Ld from chamelon stat call, path = %s" kind size path);
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
          st_ctime = u_stat.st_ctime}
        with Invalid_argument _ -> failwith "cstruct returned by chamelon stat call was invalid")

let chamelon_fsync (_path : string) (_ds : bool) fd =
  Lwt_main.run @@ (Log.app (fun f -> f "fsync call"); Lwt_unix.fsync (Lwt_unix.of_unix_file_descr (retrieve_descr fd)))

let chamelon_fopen path flags =
  Lwt_main.run @@ (
    let flag_list = List.fold_left (fun acc s -> acc ^ " " ^ (flag_to_string s)) "" flags in
    Log.app (fun f -> f "fopen call, path = %s, flags = [%s]" path flag_list);
    Littlefs.fopen path >>= fun fd_opt -> match fd_opt with
    | Some fd ->
      let d =  store_descr (Lwt_unix.unix_file_descr fd) in Lwt.return @@ Some d
    | None -> Lwt.return @@ None
  )

let chamelon_opendir path flags =
  Lwt_main.run @@ (
    let flag_list = List.fold_left (fun acc s -> acc ^ " " ^ (flag_to_string s)) "" flags in
    Log.app (fun f -> f "opendir call, path = %s, flags = %s" path flag_list);
    Lwt_unix.openfile path flags 0 >>= fun fd ->
    Lwt_unix.close fd >>= fun _ -> Lwt.return @@ None)

let chamelon_release path (_flags : Unix.open_flag list) fd =
  Lwt_main.run @@ (Log.app (fun f -> f "release call"); Lwt_unix.(close (of_unix_file_descr (retrieve_descr fd))) >>= fun _ -> Littlefs.release path)

let chamelon_readdir path fd =
  Lwt_main.run @@ (
    Log.app (fun f -> f "readdir call, path = %s, fd = %d" path fd);
      Littlefs.readdir path (retrieve_descr fd) >>= function
      | Error _ -> Log.app (fun f -> f "error reading entries from path %s with fd %d" path fd); Lwt.return @@ []
      | Ok entries -> Log.app (fun f -> f "successfully read entries from path %s with fd %d" path fd); Lwt.return @@ (List.map fst entries)
  )

let chamelon_mknod path mode =
  Lwt_main.run @@ (
    let open Unix in
    Log.app (fun f -> f "mknod call, path = %s, mode = %d" path mode);
    close (openfile path [ O_CREAT; O_EXCL ] mode);
    Lwt.return_unit)

let _ =
  main Sys.argv
    {    
      init = (fun _ -> Lwt_main.run @@ (Log.app (fun f -> f "filesystem started"); Lwt.return_unit));
      statfs = chamelon_statfs;

      getattr = chamelon_stat;

      mkdir = chamelon_mkdir;
      rmdir = chamelon_rmdir;
      opendir = chamelon_opendir;
      releasedir = chamelon_release;
      readdir = chamelon_readdir;

      link = Fuse_lib.undefined;
      unlink = chamelon_unlink;
      
      fopen = chamelon_fopen;
      release = chamelon_release;
      read = chamelon_read;
      write = chamelon_write;
      fsync = chamelon_fsync;
      flush = Fuse_lib.undefined;

      fsyncdir = chamelon_fsync;
      mknod = chamelon_mknod;

      (* unsupported operations *)
      truncate = Fuse_lib.undefined;
      rename = Fuse_lib.undefined;
      symlink = Fuse_lib.undefined;
      readlink = Fuse_lib.undefined;
      listxattr = Fuse_lib.undefined;
      getxattr = Fuse_lib.undefined;
      setxattr = Fuse_lib.undefined;
      removexattr = Fuse_lib.undefined;
      chmod = Fuse_lib.undefined;
      chown = Fuse_lib.undefined;
      utime = Fuse_lib.undefined;
    }
