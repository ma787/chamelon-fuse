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

    val mkdir : string -> Unix.file_descr -> (unit, Mirage_kv.write_error) result Lwt.t
    val rmdir : string -> unit Lwt.t
    val readdir : string -> Unix.file_descr -> ((string * [`Dictionary | `Value]) list, Mirage_kv.error) result Lwt.t
    val statfs : string -> statvfs Lwt.t
    val stat : string -> Cstruct.t Lwt.t
    val flush : string -> Unix.file_descr -> (unit, [> `write_error]) result Lwt.t
    val read : string -> Unix.file_descr -> (string * int) Lwt.t
    val write : string -> Unix.file_descr -> string -> (unit, Mirage_kv.write_error) result Lwt.t
    val fopen : string -> Lwt_unix.file_descr option Lwt.t
end 