fn main() {
    // PHP extension symbols (emalloc, zend_*, etc.) are resolved at runtime
    // when PHP `dlopen`s the shared library. The linker must be told to
    // allow undefined symbols.
    //
    // 也对测试二进制生效——`cargo test --lib` 会把 crate 链成 exe，同样需要
    // `undefined dynamic_lookup` 才能让本地纯 Rust 单测跑起来（不需要真 PHP 运行时）。
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    match target_os.as_str() {
        "macos" => {
            println!("cargo:rustc-cdylib-link-arg=-undefined");
            println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
        }
        "linux" => {
            // Linux allows undefined symbols in shared libs by default;
            // but explicit -Wl,--unresolved-symbols may be needed in some setups.
        }
        _ => {}
    }
}
