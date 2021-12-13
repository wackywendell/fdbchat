async fn hello_world() -> foundationdb::FdbResult<()> {
    let db = foundationdb::Database::default()?;

    // write a value
    let trx = db.create_trx()?;
    trx.set(b"hello", b"world"); // errors will be returned in the future result
    trx.commit().await?;

    // read a value
    let trx = db.create_trx()?;
    let maybe_value = trx.get(b"hello", false).await?;
    let value = maybe_value.unwrap(); // unwrap the option

    assert_eq!(b"world", &value.as_ref());

    Ok(())
}

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let network = unsafe { foundationdb::boot() };

    hello_world().await?;

    drop(network);
    Ok(())
}

// fn main() {
//     tokio::runtime::Builder::hello_world().await?;
// }
