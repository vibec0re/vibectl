// 🔥 VIBEC0RE WEBUI - CYBER EDITION! 💖
//
// The wasm entry point trunk builds (`index.html` has `data-bin="v1bectl_web"`).
// All the code lives in the lib's module tree. `[lib]` is `cdylib`-only, and a
// bin can't link a cdylib, so pull in `lib.rs` itself rather than re-declaring
// the modules here. Once the lib also builds as an `rlib`, this becomes
// `fn main() { v1bectl_web::run() }`.
include!("lib.rs");

fn main() {
    run();
}
