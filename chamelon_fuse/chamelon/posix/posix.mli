module Make(Sectors: Mirage_block.S)(Clock : Mirage_clock.PCLOCK) : sig
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

    val mkdir : string -> int -> (unit, Mirage_kv.write_error) result Lwt.t
    val remove : string -> unit Lwt.t
    val readdir : string -> ((string * [`Dictionary | `Value]) list, Mirage_kv.error) result Lwt.t
    val statfs : string -> statvfs Lwt.t
    val stat : string -> Cstruct.t Lwt.t
    val read : string -> (string * int) Lwt.t
    val write : string -> string -> (unit, Mirage_kv.write_error) result Lwt.t
    val fopen : string -> Lwt_unix.file_descr option Lwt.t
    val mknod : string -> unit Lwt.t
end 