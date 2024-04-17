(* unfortunately, "block" is a really overloaded term in this codebase :/ *)

(* the module providing Mirage_block.S expects operations to be specified in
 * terms of *sectors*, which is not the level on which littlefs wants to
 * operate natively - we want to address blocks.
 * Provide an intermediate interface that implements block operations on top of
 * the sector-based API provided by a Mirage_block.S implementor. *)

module Make(Sectors : Mirage_block.S) : sig
  include Mirage_block.S
  val to_t : Sectors.t -> t
  val to_sectors : t -> Sectors.t
  val block_count : t -> int
end = struct
  type t = {
    mutable fd: Lwt_unix.file_descr option;
    mutable seek_offset: int64;
    (* a shadow copy of the fd's seek offset which avoids calling `lseek`
      unnecessarily, speeding up sequential read and write *)
    m: Lwt_mutex.t;
    mutable info: Mirage_block.info;
    config: Config.t;
    use_fsync_after_write: bool;
}

  let block_size = 512

  type error = Sectors.error
  type write_error = Sectors.write_error

  let pp_error = Sectors.pp_error
  let pp_write_error = Sectors.pp_write_error

  let log_src = Logs.Src.create "chamelon-shim" ~doc:"chamelon block-to-sector shim"
  module Log = (val Logs.src_log log_src : Logs.LOG)

  let get_info t = Lwt.return t.info

  let sector_of_block t n =
    let byte_of_n = Int64.(mul n @@ of_int block_size) in
    Int64.(div byte_of_n @@ of_int t.info.sector_size)

  let block_count t =
    (Int64.to_int t.info.size_sectors) * t.info.sector_size / block_size

  let create fd seek_offset m info config use_fsync_after_write = {fd; seek_offset; m; info; config; use_fsync_after_write}
  
  let get_elems t = t.fd, t.seek_offset, t.m, t.info, t.config, t.use_fsync_after_write

  let to_sectors t  = 
  let fd, seek_offset, m, info, config, use_fsync_after_write = get_elems t in
  Sectors.create fd seek_offset m info config use_fsync_after_write
  
  let to_t t = 
    let fd, seek_offset, m, info, config, use_fsync_after_write = Sectors.get_elems t in
    {fd; seek_offset; m; info; config; use_fsync_after_write}

  let connect ?buffered ?sync ?lock ?prefered_sector_size path = 
    let open Lwt.Infix in
    Sectors.connect ?buffered ?sync ?lock ?prefered_sector_size path >>= fun block -> Lwt.return @@ (to_t block)

  let disconnect t = Sectors.disconnect (to_sectors t)

  let of_fd path fd = 
    let open Lwt.Infix in
    Sectors.of_fd path fd >>= fun t -> Lwt.return @@ to_t t
  
  let flush t = Sectors.flush (to_sectors t)

  let write t block_number = Sectors.write (to_sectors t) (sector_of_block t block_number)
  let read t block_number = Sectors.read (to_sectors t) (sector_of_block t block_number)
end
