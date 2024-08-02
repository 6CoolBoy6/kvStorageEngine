

use std::io::{self, Read, Write};
use std::net::TcpStream;

fn main() -> io::Result<()> {
    let server_address = "127.0.0.1:9096";

    let mut stream = TcpStream::connect(server_address)?;

    stream.write_all(b"GET NameRust")?;

    let mut buffer = [0; 1024];
    let bytes_read = stream.read(&mut buffer)?;
    println!("Received: {}", String::from_utf8_lossy(&buffer[..bytes_read]));

    Ok(())
}
