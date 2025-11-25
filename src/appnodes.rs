
use crate::clientnode::{ConstantNode, METH_PING};
use shvrpc::metamethod::{AccessLevel, Flag, MetaMethod};
use shvrpc::{RpcMessageMetaTags, RpcMessage, rpcmessage::RpcError};
use shvproto::RpcValue;

const METH_SHV_VERSION_MAJOR: &str = "shvVersionMajor";
const METH_SHV_VERSION_MINOR: &str = "shvVersionMinor";
const METH_NAME: &str = "name";
const METH_VERSION: &str = "version";
const METH_SERIAL_NUMBER: &str = "serialNumber";

const SHV_VERSION_MAJOR: i32 = 3;
const SHV_VERSION_MINOR: i32 = 0;

pub const DOT_APP_METHODS: &[&MetaMethod] = &[
    &MetaMethod::new_static(
        METH_SHV_VERSION_MAJOR,
        Flag::IsGetter as u32,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    &MetaMethod::new_static(
        METH_SHV_VERSION_MINOR,
        Flag::IsGetter as u32,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    &MetaMethod::new_static(
        METH_NAME,
        Flag::IsGetter as u32,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    &MetaMethod::new_static(
        METH_PING,
        Flag::None as u32,
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

impl ConstantNode for DotAppNode {
    fn methods(&self) -> Vec<&MetaMethod> {
        DOT_APP_METHODS.to_vec()
    }

    fn process_request(&self, request: &RpcMessage) -> Option<Result<RpcValue, RpcError>> {
        match request.method() {
            Some(METH_SHV_VERSION_MAJOR) => Some(self.shv_version_major.into()),
            Some(METH_SHV_VERSION_MINOR) => Some(self.shv_version_minor.into()),
            Some(METH_NAME) => Some(RpcValue::from(&self.app_name)),
            Some(METH_PING) => Some(().into()),
            _ => None,
        }.map(Ok)
    }
}

pub const DOT_DEVICE_METHODS: &[&MetaMethod] = &[
    &MetaMethod::new_static(
        METH_NAME,
        Flag::IsGetter as u32,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    &MetaMethod::new_static(
        METH_VERSION,
        Flag::IsGetter as u32,
        AccessLevel::Browse,
        "",
        "",
        &[],
        "",
    ),
    &MetaMethod::new_static(
        METH_SERIAL_NUMBER,
        Flag::IsGetter as u32,
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

impl ConstantNode for DotDeviceNode {
    fn methods(&self) -> Vec<&MetaMethod> {
        DOT_DEVICE_METHODS.to_vec()
    }

    fn process_request(&self, request: &RpcMessage) -> Option<Result<RpcValue, RpcError>> {
        match request.method() {
            Some(METH_NAME) => Some(RpcValue::from(&self.device_name)),
            Some(METH_VERSION) => Some(RpcValue::from(&self.version)),
            Some(METH_SERIAL_NUMBER) => match &self.serial_number {
                None => Some(RpcValue::null()),
                Some(sn) => Some(RpcValue::from(sn)),
            },
            _ => None,
        }.map(Ok)
    }
}

