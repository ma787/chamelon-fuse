type sync_behaviour = [
  | `ToOS
  | `ToDrive
]

let sync_behaviour_of_string = function
  | "0" | "none" -> None
  | "1" | "drive" -> Some `ToDrive
  | "os" -> Some `ToOS
  | _ -> None

let string_of_sync = function
  | None -> "none"
  | Some `ToDrive -> "drive"
  | Some `ToOS -> "os"

type t = {
  buffered: bool;
  sync: sync_behaviour option;
  path: string;
  lock: bool;
  prefered_sector_size : int option;
}

let create ?(buffered = true) ?(sync = Some `ToOS) ?(lock = false)
    ?(prefered_sector_size = None) path =
  { buffered; sync; path; lock; prefered_sector_size }

let to_string t =
  let query = [
    "buffered", [ if t.buffered then "1" else "0" ];
    "sync",     [ string_of_sync t.sync ];
    "lock",     [ if t.lock then "1" else "0" ];
  ] in
  let u = Uri.make ~scheme:"file" ~path:t.path ~query () in
  Uri.to_string u

let of_string x =
  let u = Uri.of_string x in
  match Uri.scheme u with
  | Some "file" ->
    let query = Uri.query u in
    let buffered = try List.assoc "buffered" query = [ "1" ] with Not_found -> false in
    let sync     = try sync_behaviour_of_string @@ List.hd @@ List.assoc "sync" query with Not_found -> None in
    let lock     = try List.assoc "lock" query = [ "1" ] with Not_found -> false in
    let prefered_sector_size =
      try Some (int_of_string @@ List.hd @@ List.assoc "prefered_sector_size" query) with Not_found -> None
    in
    let path = Uri.(pct_decode @@ path u) in
    Ok { buffered; sync; path; lock; prefered_sector_size }
  | _ ->
    Error (`Msg "Config.to_string expected a string of the form file://<path>?sync=(none|os|drive)&buffered=(0|1)&lock=(0|1)")