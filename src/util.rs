use md5::{Digest, Md5};

pub fn encode_md5_password_hash(username: &str, password: &str, salt: &[u8]) -> String {
    let mut md5 = Md5::new();
    md5.update(password.as_bytes());
    md5.update(username.as_bytes());
    let output = md5.finalize_reset();
    md5.update(format!("{:x}", output));
    md5.update(&salt);
    format!("md5{:x}", md5.finalize())
}
