
/// Generator for static nodes
///
/// A convenient macro for generating static nodes with methods table, handlers,
/// automatic parameters checking and response generation at one place.
///
/// Usage:
///
///#```
///#  let node = static_node!{
///#         NodeTypeName(request, client_cmd_tx) {
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
macro_rules! static_node {
    (
        $name:ident ( $request:ident , $client_cmd_tx:ident ) {
            $(
                $method:tt
                [ $($flags:ident)|* , $access:ident , $methodparam:expr , $methodresult:expr ]
                $(
                    { $( ($signame:expr, $sigval:expr) ),* $(,)? }
                )?
                $( ( $param:ident : $ptype:ty ) )?
                => $body:block
            )+
        }
    ) => {
        {
            pub struct $name;

            $crate::impl_static_node!(
                $name ( &self, $request, $client_cmd_tx ) {
                    $(
                        $method
                        [ $($flags)|* , $access, $methodparam, $methodresult ]
                        $(
                            { $( ($signame, $sigval) ),* }
                        )?
                        $( ( $param: $ptype) )?
                        => $body
                    )+
                }
            );
            $name
        }
    }
}

/// `impl_static_node` macro generates a `StaticNode` trait impl block for a custom type.
///
/// The syntax is equivalent to the `static_node` macro, it just takes one
/// extra parameter `&self` that is provided in the method handlers.
///
/// Unlike `static_node` this macro does not generate any expression, only
/// the impl block.
#[macro_export]
macro_rules! impl_static_node {
    (
        $type:ident ( & $self_ident:ident , $request:ident , $client_cmd_tx:ident ) {
            $(
                $method:tt
                [ $($flags:ident)|* , $access:ident , $methodparam:expr , $methodresult:expr ]
                $(
                    { $( ($signame:expr, $sigval:expr) ),* $(,)? }
                )?
                $( ( $param:ident : $ptype:ty ) )?
                => $body:block
            )+
        }
    ) => {

        #[async_trait::async_trait]
        impl $crate::clientnode::StaticNode for $type {

            fn methods(&self) -> &'static [$crate::clientnode::MetaMethod] {
                const METHODS: &[$crate::clientnode::MetaMethod] =
                &[
                    $(
                        $crate::clientnode::MetaMethod::new_static(
                            $method,
                            $crate::clientnode::Flags::from_bits_retain(0 $(| $crate::clientnode::Flags::$flags.bits() )* ),
                            $crate::clientnode::AccessLevel::$access,
                            $methodparam,
                            $methodresult,
                            &[
                                $($(($signame, $sigval)),*)?
                            ],
                            "",
                        )
                    ),+
                ];
                METHODS
            }

            async fn process_request(
                &$self_ident,
                $request: $crate::shvrpc::rpcmessage::RpcMessage,
                $client_cmd_tx: $crate::ClientCommandSender,
            ) -> Option<$crate::clientnode::RequestResult>
            {
                use $crate::shvrpc::RpcMessageMetaTags;

                match $request.method() {
                    $(
                        Some($method) => {
                            $crate::method_handler!(
                                $( ($param : $ptype) )?
                                $method @ $request @ $body
                            )
                        }
                    )+

                    _ => Some(Err($crate::clientnode::RpcError::new(
                        $crate::clientnode::RpcErrorCode::MethodNotFound,
                        format!("Invalid method: {:?}", $request.method())
                    ))),
                }
            }
        }
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
                                 format!("Wrong parameter for `{method}`: {err}",
                                     method = $method
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
