pub fn ws_to_http(ws: &str) -> Option<String> {
    let mut u = reqwest::Url::parse(ws).ok()?;
    let scheme = u.scheme().to_owned();

    let new_scheme = match scheme.as_str() {
        "wss" => "https",
        "ws" => "http",
        other => other,
    };

    u.set_scheme(new_scheme).ok()?;
    Some(u.into())
}
