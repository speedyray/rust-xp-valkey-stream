use redis::Commands;
use redis::streams::{StreamReadOptions, StreamReadReply};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	println!("Hello, World");
	let client = redis::Client::open("redis://127.0.0.1/")?;
	let mut con = client.get_connection()?;

	let stream_name = "stream-c02";

	// -- Add
	let _: () = con.xadd(stream_name, "*", &[("name", "Zimba"), ("surname", "Toola")])?;

	// -- Read all Stream Records from the start
	let res: StreamReadReply = con.xread(&[stream_name], &["0"])?;

	println!("All records for {stream_name}: {res:#?} ");

	// -- Read only one record
	let options = StreamReadOptions::default().count(1);
	let res: StreamReadReply = con.xread_options(&[stream_name], &["0"], &options)?;
	println!("Only one record for {stream_name}: {res:?} ");

	Ok(())
}