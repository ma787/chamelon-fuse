type sync_behaviour = [
    | `ToOS (** flush to the operating system, not necessarily the drive *)
    | `ToDrive (** flush to the drive *)
]

val string_of_sync: sync_behaviour option -> string

type t = {
    buffered: bool; (** true if I/O hits the OS disk caches, false if "direct" *)
    sync: sync_behaviour option;
    path: string; (** path to the underlying file *)
    lock: bool; (** true if the file should be locked preventing concurrent modification *)
    prefered_sector_size : int option;
        (** the size of sectors when it cannot be determined automatically *)
}
(** Configuration of a device *)

val create :
    ?buffered:bool ->
    ?sync:sync_behaviour option ->
    ?lock:bool ->
    ?prefered_sector_size:int option ->
    string ->
    t
(** [create ?buffered ?sync ?lock path] constructs a configuration referencing
    the file stored at [path]. *)

val to_string: t -> string
(** Marshal a config into a string of the form
    file://<path>?sync=(0|1)&buffered=(0|1) *)

val of_string: string -> (t, [`Msg of string ]) result
(** Parse the result of a previous [to_string] invocation *)