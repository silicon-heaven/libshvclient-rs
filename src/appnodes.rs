
use crate::clientnode::{StaticNode, METH_PING};
use crate::ClientCommandSender;
use async_trait::async_trait;
use shvrpc::metamethod::{AccessLevel, Flags, MetaMethod};
use shvrpc::rpcmessage::RpcErrorCode;
use shvrpc::{RpcMessageMetaTags, RpcMessage, rpcmessage::RpcError};
use shvproto::RpcValue;

const METH_SHV_VERSION_MAJOR: &str = "shvVersionMajor";
const METH_SHV_VERSION_MINOR: &str = "shvVersionMinor";
const METH_NAME: &str = "name";
const METH_DATE: &str = "date";
const METH_VERSION: &str = "version";
const METH_SERIAL_NUMBER: &str = "serialNumber";

const SHV_VERSION_MAJOR: i32 = 3;
const SHV_VERSION_MINOR: i32 = 0;

pub const DOT_APP_METHODS: &[MetaMethod] = &[
    MetaMethod::new_static(
        METH_SHV_VERSION_MAJOR,
        Flags::IsGetter,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    MetaMethod::new_static(
        METH_SHV_VERSION_MINOR,
        Flags::IsGetter,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    MetaMethod::new_static(
        METH_NAME,
        Flags::IsGetter,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    MetaMethod::new_static(
        METH_DATE,
        Flags::IsGetter,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    MetaMethod::new_static(
        METH_PING,
        Flags::None,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
];

pub struct DotAppNode {
    app_name: String,
    shv_version_major: i32,
    shv_version_minor: i32,
}

impl DotAppNode {
    pub fn new(app_name: impl Into<String>) -> Self {
        Self {
            app_name: app_name.into(),
            shv_version_major: SHV_VERSION_MAJOR,
            shv_version_minor: SHV_VERSION_MINOR,
        }
    }
}

fn err_method_not_found() -> RpcError {
    RpcError::new(RpcErrorCode::MethodNotFound, "method not found")
}

#[async_trait]
impl StaticNode for DotAppNode {
    fn methods(&self) -> &'static [MetaMethod] {
        DOT_APP_METHODS
    }

    async fn process_request(&self, request: RpcMessage, _: ClientCommandSender) -> Option<Result<RpcValue, RpcError>> {
        Some(match request.method() {
            Some(METH_SHV_VERSION_MAJOR) => Ok(self.shv_version_major.into()),
            Some(METH_SHV_VERSION_MINOR) => Ok(self.shv_version_minor.into()),
            Some(METH_NAME) => Ok(RpcValue::from(&self.app_name)),
            Some(METH_DATE) => Ok(shvproto::DateTime::now().into()),
            Some(METH_PING) => Ok(().into()),
            _ => Err(err_method_not_found()),
        })
    }
}

pub const DOT_DEVICE_METHODS: &[MetaMethod] = &[
    MetaMethod::new_static(
        METH_NAME,
        Flags::IsGetter,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    MetaMethod::new_static(
        METH_VERSION,
        Flags::IsGetter,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    MetaMethod::new_static(
        METH_SERIAL_NUMBER,
        Flags::IsGetter,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
];

pub struct DotDeviceNode {
    device_name: String,
    version: String,
    serial_number: Option<String>,
}

impl DotDeviceNode {
    pub fn new(device_name: impl Into<String>, version: impl Into<String>, serial_number: impl Into<Option<String>>) -> Self {
        Self {
            device_name: device_name.into(),
            version: version.into(),
            serial_number: serial_number.into(),
        }
    }
}

#[async_trait]
impl StaticNode for DotDeviceNode {
    fn methods(&self) -> &'static [MetaMethod] {
        DOT_DEVICE_METHODS
    }

    async fn process_request(&self, request: RpcMessage, _:ClientCommandSender) -> Option<Result<RpcValue, RpcError>> {
        Some(match request.method() {
            Some(METH_NAME) => Ok(RpcValue::from(&self.device_name)),
            Some(METH_VERSION) => Ok(RpcValue::from(&self.version)),
            Some(METH_SERIAL_NUMBER) => Ok(self.serial_number.as_ref().map_or_else(RpcValue::null, RpcValue::from)),
            _ => Err(err_method_not_found()),
        })
    }
}

// #[async_trait]
// impl StaticNode for DeviceNode {
//     fn methods(&self) -> &'static [MetaMethod] {
//         &[
//             MetaMethod::new_static(
//                 "something",
//                 Flag::IsGetter as u32,
//                 AccessLevel::Browse,
//                 "",
//                 "",
//                 &[],
//                 "",
//             ),
//             MetaMethod::new_static(
//                 "get",
//                 Flag::IsGetter as u32,
//                 AccessLevel::Browse,
//                 "",
//                 "",
//                 &[],
//                 "",
//             ),]
//     }
//
//     async fn process_request(&self, request: RpcMessage, tx: ClientCommandSender) -> Option<Result<RpcValue, RpcError>> {
//         match request.method() {
//             Some("something") => {
//                 let request_param = request.param().unwrap_or_default();
//
//                 match <i32>::try_from(request_param) {
//                     Ok(param) => {
//                         println!("param: {param}");
//                         Some(Ok(RpcValue::from("name result")))
//                     }
//                     Err(err) => Some(Err(crate::clientnode::RpcError::new(
//                                 crate::clientnode::RpcErrorCode::InvalidParam,
//                                 format!("Wrong parameter for `{}`: {}",
//                                     "something",
//                                     err
//                                 ))))
//                 }
//             }
//             Some("get") => {
//                 Some(Ok(RpcValue::from(42)))
//             }
//             _ => Some(Err(err_method_not_found())),
//         }
//     }
// }
