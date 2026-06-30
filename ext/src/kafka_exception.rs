//! `Hi\Kafka\KafkaException` —— 所有 Kafka 操作失败抛出的统一异常。
//!
//! **为何手动 `ClassBuilder` 注册而非 `#[php_class]`**：
//! ext-php-rs 的 `#[php_class]` 为承载 Rust struct，**必然覆盖类的 `create_object`**
//! 处理器（用自己的 `new_uninit` 分配对象）。而 PHP 的 `\Exception` 恰恰是在
//! `create_object`（`zend_default_exception_new`）里捕获**调用栈 / 文件 / 行号**的——
//! 被覆盖后 `getTrace()` 永远为空、`getFile()`/`getLine()` 也拿不到。
//! 这里用 `ClassBuilder` 注册一个**不覆盖 `create_object`** 的类，让它继承 `\Exception`
//! 的对象创建器，从而 `new`/抛出时正常捕获调用栈。
//!
//! 机器可读分类以**公开属性**暴露：`kind` / `kind_name` / `retryable` / `native_code`
//! （并复用 `\Exception` 的 `code`(= kind) 与 `message`），同时保留 `getKind()` /
//! `getKindName()` / `isRetryable()` / `getNativeCode()` 四个 getter（与 composer 桩
//! `php-driver/src/Hi/Kafka/KafkaException.php` 对齐，避免装/不装扩展时 API 漂移）。

use ext_php_rs::builders::{ClassBuilder, FunctionBuilder};
use ext_php_rs::convert::IntoZval;
use ext_php_rs::flags::{DataType, MethodFlags, PropertyFlags};
use ext_php_rs::types::Zval;
use ext_php_rs::zend::{ce, ClassEntry, ExecuteData};
use ext_php_rs::zend_fastcall;
use std::sync::OnceLock;

/// Newtype 包 `&'static ClassEntry` 加 `Sync`/`Send`——ClassEntry 含 raw pointer，
/// 但 MINIT 完成后只读、生命周期等于模块，跨线程读取安全（同 `client_interface`）。
struct CeRef(&'static ClassEntry);
// SAFETY: 见上
unsafe impl Sync for CeRef {}
unsafe impl Send for CeRef {}

static EXC_CE: OnceLock<CeRef> = OnceLock::new();

// === getter 方法（读 $this 公开属性）===
// 手动 ClassBuilder 没有 Rust struct，方法经 ExecuteData 拿 $this、读属性、写返回值。
// 不设自定义 `__construct`：继承 `\Exception` 的 `($message, $code = kind)`，分类信息由
// 抛出方（`build_zval` / 协程 driver `makeKafka`）在构造后写入公开属性。

zend_fastcall! {
    extern fn get_kind(ex: &mut ExecuteData, retval: &mut Zval) {
        let v = ex
            .get_self()
            .and_then(|o| o.get_property::<i64>("kind").ok())
            .unwrap_or(0);
        retval.set_long(v);
    }
}

zend_fastcall! {
    extern fn get_kind_name(ex: &mut ExecuteData, retval: &mut Zval) {
        let v = ex
            .get_self()
            .and_then(|o| o.get_property::<String>("kind_name").ok())
            .unwrap_or_default();
        let _ = retval.set_string(&v, false);
    }
}

zend_fastcall! {
    extern fn is_retryable(ex: &mut ExecuteData, retval: &mut Zval) {
        let v = ex
            .get_self()
            .and_then(|o| o.get_property::<bool>("retryable").ok())
            .unwrap_or(false);
        retval.set_bool(v);
    }
}

zend_fastcall! {
    extern fn get_native_code(ex: &mut ExecuteData, retval: &mut Zval) {
        let v = ex
            .get_self()
            .and_then(|o| o.get_property::<i64>("native_code").ok())
            .unwrap_or(0);
        retval.set_long(v);
    }
}

/// MINIT 阶段注册 `Hi\Kafka\KafkaException`。幂等。
pub fn register() {
    if EXC_CE.get().is_some() {
        return;
    }
    let get_kind_m = FunctionBuilder::new("getKind", get_kind)
        .returns(DataType::Long, false, false)
        .build()
        .expect("build getKind");
    let get_kind_name_m = FunctionBuilder::new("getKindName", get_kind_name)
        .returns(DataType::String, false, false)
        .build()
        .expect("build getKindName");
    let is_retryable_m = FunctionBuilder::new("isRetryable", is_retryable)
        .returns(DataType::Bool, false, false)
        .build()
        .expect("build isRetryable");
    let get_native_code_m = FunctionBuilder::new("getNativeCode", get_native_code)
        .returns(DataType::Long, false, false)
        .build()
        .expect("build getNativeCode");

    let ce = ClassBuilder::new("Hi\\Kafka\\KafkaException")
        .extends(ce::exception())
        .property("kind", 0i64, PropertyFlags::Public)
        .property("kind_name", "", PropertyFlags::Public)
        .property("retryable", false, PropertyFlags::Public)
        .property("native_code", 0i64, PropertyFlags::Public)
        .method(get_kind_m, MethodFlags::Public)
        .method(get_kind_name_m, MethodFlags::Public)
        .method(is_retryable_m, MethodFlags::Public)
        .method(get_native_code_m, MethodFlags::Public)
        .build()
        .expect("failed to register Hi\\Kafka\\KafkaException");
    let _ = EXC_CE.set(CeRef(ce));
}

/// 构造一个**带真实调用栈**的 KafkaException 对象 zval（供 Rust 抛出路径用，
/// 见 `lib.rs::ipc_err_to_php`）。
///
/// 流程：
/// 1. `ce.new()` 经（继承自 `\Exception` 的）`create_object` 创建对象 → 捕获 trace/file/line；
/// 2. 调（继承的）`\Exception::__construct(message, code=kind)` 设 message/code；
/// 3. `set_property` 设 4 个公开分类属性。
///
/// 任一步失败返回 `None`，调用方回退到通用 `PhpException`（至少消息不丢）。
pub fn build_zval(
    message: &str,
    kind: i64,
    kind_name: &str,
    retryable: bool,
    native_code: i64,
) -> Option<Zval> {
    let ce = EXC_CE.get().map(|r| r.0)?;
    // ce.new() 经继承自 \Exception 的 create_object 创建对象 ZBox<ZendObject> → 捕获调用栈。
    let mut obj = ce.new();
    // owned 绑定确保参数 zval 生命周期覆盖调用。
    let msg = message.to_string();
    obj.try_call_method("__construct", vec![&msg, &kind]).ok()?;
    obj.set_property("kind", kind).ok()?;
    obj.set_property("kind_name", kind_name).ok()?;
    obj.set_property("retryable", retryable).ok()?;
    obj.set_property("native_code", native_code).ok()?;
    obj.into_zval(false).ok()
}
