
/// Generator for fixed nodes
///
/// A convenient macro for generating fixed nodes with methods table,
/// handlers, automatic parameters checking and response generation
/// at one place.
///
/// Usage:
///
///#```
///#  let node = fixed_node!{
///#         // `app_state` parameter is optional and has to be specified with the type parameter.
///#         device_handler(request, client_cmd_tx, app_state: i32) {
///#             // If a parameter in ( ) is present, the code will handle the type
///#             // conversion and send an appropriate Error response on a failure.
///#             // The type has to implements trait `TryFrom<&RpcValue, Error=String>`.
///#             "name" [IsGetter, Browse] (param: i32) => {
///#                 println!("param: {}", param);
///#                 app_state.map(|v| { println!("app_state: {}", *v); });
///#                 Some(Ok(RpcValue::from("name result")))
///#             }
///#             "echo" [IsGetter, Browse] (param: Vec<String>) => {
///#                 for s in &param {
///#                     if s.contains("foo") {
///#                         // Return statements are supported
///#                         return Some(Err(shvrpc::rpcmessage::RpcError::new(
///#                                     shvrpc::rpcmessage::RpcErrorCode::InvalidParam,
///#                                     "err".to_string()))
///#                         );
///#                     }
///#                     println!("param item: {}", &s);
///#                 }
///#                 Some(Ok(param.into()))
///#             }
///#             "setTable" [IsGetter, Browse] (table: MyTable) => {
///#                 handle_table(table, client_cmd_tx);
///#                 // The response is sent in `handle_table` above, so we return `None`
///#                 // to indicate that the generated code shouldn't send the response.
///#                 None
///#             }
///#             "version" [IsGetter, Browse] => {
///#                 Some(Ok(RpcValue::from(42)))
///#             }
///#         }
///#     }
///# }
///# ```
#[macro_export]
macro_rules! fixed_node {
    ($fn_name:ident < $T:ty > ( $request:ident, $client_cmd_tx:ident $(, $app_state:ident)?) {
        $($method:tt [$($flags:ident)|+, $access:ident, $methodparam:expr, $methodresult:expr] $({ $(($signame:expr, $sigval:expr)),* })? $(($param:ident : $type:ty ))? => $body:block)+
    }) => {

        {
            const METHODS: &[$crate::clientnode::MetaMethod] = &[
                $($crate::clientnode::MetaMethod::new_static(
                    $method,
                    $($crate::clientnode::Flag::$flags as u32)|+,
                    $crate::clientnode::AccessLevel::$access,
                    $methodparam,
                    $methodresult,
                    &[$($(($signame, $sigval)),*)?],
                    "",
                ),)+
            ];

            async fn $fn_name($request: $crate::shvrpc::rpcmessage::RpcMessage, $client_cmd_tx: $crate::ClientCommandSender<$T> $(, $app_state: Option<$crate::AppState<$T>>)?) {

                use $crate::shvrpc::RpcMessageMetaTags;

                if $request.shv_path().unwrap_or_default().is_empty() {
                    let mut __resp = $request.prepare_response().unwrap_or_default();
                    $(
                        let Some($app_state) = $app_state else {
                            log::error!("{}: Application state should be Some", stringify!($fn_name));
                            __resp.set_error($crate::shvrpc::rpcmessage::RpcError::new($crate::shvrpc::rpcmessage::RpcErrorCode::InternalError, "Ill-formed method implementation"));
                            if let Err(e) = $client_cmd_tx.send_message(__resp) {
                                log::error!("{}: Cannot send response ({e})", stringify!($fn_name));
                            }
                            return;
                        };
                    )?

                    async fn handler($request: $crate::shvrpc::rpcmessage::RpcMessage, $client_cmd_tx: $crate::ClientCommandSender<$T> $(, $app_state: $crate::AppState<$T>)?)
                    -> Option<std::result::Result<$crate::clientnode::RpcValue, $crate::clientnode::RpcError>> {
                        match $request.method() {

                            $(Some($method) => {
                                $crate::method_handler!($(($param : $type))? $method @ $request @ $body)
                            })+

                            _ => Some(Err($crate::clientnode::RpcError::new(
                                        $crate::clientnode::RpcErrorCode::MethodNotFound,
                                        format!("Invalid method: {:?}", $request.method())))
                            )
                        }
                    }

                    if let Some(val) = handler($request, $client_cmd_tx.clone() $(, $app_state)?).await {
                        if let Ok(res) = val {
                            __resp.set_result(res);
                        } else if let Err(err) = val {
                            __resp.set_error(err);
                        }

                        if let Err(e) = $client_cmd_tx.send_message(__resp) {
                            log::error!("{}: Cannot send response ({e})", stringify!($fn_name));
                        }
                    }
                };
            }

            $crate::clientnode::ClientNode::fixed(
                METHODS,
                [$crate::clientnode::Route::new(
                    [$($method),+],
                    $crate::request_handler!($fn_name $(,$app_state)?),
                )]
            )
        }
    }
}

#[macro_export]
macro_rules! request_handler {
    ($fn_name:ident) => {
        $crate::RequestHandler::stateless($fn_name)
    };
    ($fn_name:ident, $app_state:ident) => {
        $crate::RequestHandler::stateful($fn_name)
    };
}

#[macro_export]
macro_rules! method_handler {
    (($param:ident : $type:ty) $method:tt @ $request:ident @ $body:block) => {
        {
            let request_param = $request.param().unwrap_or_default();

             match <$type>::try_from(request_param) {
                 Ok($param) => $body,
                 Err(err) => Some(Err($crate::clientnode::RpcError::new(
                                 $crate::clientnode::RpcErrorCode::InvalidParam,
                                 format!("Wrong parameter for `{}`: {}",
                                     $method,
                                     err
                                 ))))
            }
        }
    };
    ($method:tt @ $request:ident @ $body:block) => {
        $body
    };
}

#[macro_export]
macro_rules! count {
    () => (0usize);
    ( $x:tt $($xs:tt)* ) => (1usize + $crate::count!($($xs)*));
}
