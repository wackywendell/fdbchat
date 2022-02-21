use std::any::{self, Any};
use std::fmt::Display;
use std::str::from_utf8;
use std::sync::Arc;
use std::{fmt::format, slice::SliceIndex};

use anyhow::Context;
use async_std::io;
use clap::Parser;
use foundationdb::tuple::{pack, unpack, Subspace};
use foundationdb::{Database, FdbError, FdbResult, RangeOption, Transaction};
use futures::{
    future::FutureExt, // for `.fuse()`
    select,
};
use uuid::Uuid;

type DateTime = chrono::DateTime<chrono::Utc>;

#[derive(Debug)]
pub enum AnyErr {
    Any(anyhow::Error),
    Fdb(FdbError),
}

impl From<anyhow::Error> for AnyErr {
    fn from(err: anyhow::Error) -> Self {
        AnyErr::Any(err)
    }
}

impl From<FdbError> for AnyErr {
    fn from(err: FdbError) -> Self {
        AnyErr::Fdb(err)
    }
}

impl Display for AnyErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyErr::Any(e) => e.fmt(f),
            AnyErr::Fdb(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for AnyErr {}

pub type AnyResult<T> = Result<T, AnyErr>;

impl foundationdb::TransactError for AnyErr {
    fn try_into_fdb_error(self) -> Result<FdbError, Self> {
        match self {
            AnyErr::Any(_) => Err(self),
            AnyErr::Fdb(e) => Ok(e),
        }
    }
}

const ChatOpts: foundationdb::TransactOption = foundationdb::TransactOption {
    retry_limit: Some(3),
    time_out: None,
    is_idempotent: false,
};

struct Input {
    stdin: io::Stdin,
    line: String,
}

impl Input {
    fn new() -> Input {
        Input {
            stdin: io::stdin(),
            line: String::new(),
        }
    }

    async fn next(&mut self) -> io::Result<String> {
        self.stdin.read_line(&mut self.line).await?;
        let line = std::mem::take(&mut self.line);
        Ok(line)
    }
}

pub struct Session {
    db: Arc<foundationdb::Database>,
    room: String,
    username: String,
    id: Option<Uuid>,
}

impl Session {
    fn user_key<'a>(room: &'a str, username: &'a str) -> (&'a str, &'a str, &'a str, &'a str) {
        ("rooms", room, "users", username)
    }

    async fn init_tx(tx: &Transaction, room: &str, username: &str) -> AnyResult<()> {
        // let key = foundationdb::tuple::pack(&("rooms", room, "users", username));
        let key = Session::user_key(room, username);
        let val = tx.get(&pack(&key), true).await?;

        if let Some(u) = val {
            return Err(
                anyhow::format_err!("Key {:?} already taken: {:?}", key, u.as_ref()).into(),
            );
        };

        Ok(())
    }

    async fn init(db: Arc<Database>, room: String, username: String) -> AnyResult<Self> {
        let id = Uuid::new_v4();

        // db.transact_boxed(
        //     (room.as_ref(), username.as_ref()),
        //     move |tx: &Transaction, (room, username)| {
        //         Session::init_tx(tx, room, username).boxed()
        //     },
        //     ChatOpts,
        // )
        // .await?;

        db.transact_boxed_local(
            (room.as_ref(), username.as_ref()),
            move |tx: &Transaction, (room, username)| {
                Session::init_tx(tx, room, username).boxed_local()
            },
            ChatOpts,
        )
        .await?;

        Ok(Session {
            db,
            room,
            username,
            id: Some(id),
        })
    }

    async fn leave_tx(&mut self, tx: &Transaction) -> AnyResult<()> {
        let id = match self.id {
            None => return Ok(()),
            Some(v) => v,
        };
        let key = ("rooms", &self.room, "users", &self.username);
        let keyp = pack(&key);
        let val = tx.get(&keyp, true).await?;

        let dbid: Uuid = match val {
            Some(v) => unpack(&v).map_err(anyhow::Error::from)?,
            None => return Err(anyhow::format_err!("Key is unset somehow").into()),
        };

        if dbid != id {
            return Err(anyhow::format_err!("Unexpected ID").into());
        }
        // self.db.transact_boxed_local(data, f, options)

        tx.clear(&keyp);

        self.id = None;

        Ok(())
    }

    pub async fn leave(mut self) -> AnyResult<()> {
        let db = self.db.clone();
        db.transact_boxed_local(
            &mut self,
            |tx: &Transaction, slf| slf.leave_tx(tx).boxed_local(),
            ChatOpts,
        )
        .await
    }

    fn message_key<'a>(room: &'a str, dt: DateTime) -> (&'a str, &'a str, &'a str, String) {
        (
            "rooms",
            room,
            "messages",
            dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        )
    }

    pub async fn write(&self, dt: DateTime, message: &str) -> AnyResult<()> {
        let key = Session::message_key(&self.room, dt);

        let result = self
            .db
            .transact_boxed_local(
                (key, message),
                |tx, (key, message)| {
                    async move {
                        tx.set(&pack(key), message.as_bytes());
                        Ok(())
                    }
                    .boxed_local()
                },
                ChatOpts,
            )
            .await;

        result
    }

    pub async fn read_all(&self) -> AnyResult<Vec<(DateTime, String)>> {
        let space = Subspace::from(&("rooms", &self.room, "messages"));

        let r = RangeOption::from(&space);

        let kvs = self
            .db
            .transact_boxed_local(
                &r,
                |tx, r| tx.get_range(&r, 1, true).boxed_local(),
                ChatOpts,
            )
            .await?;

        let messages: AnyResult<Vec<(DateTime, String)>> = kvs
            .iter()
            .map(|kv| {
                let k = from_utf8(kv.key()).context("Converting key");
                let k = k
                    .and_then(|k| {
                        chrono::DateTime::parse_from_rfc3339(k)
                            .map(DateTime::from)
                            .context("Parsing date")
                    })
                    .context("Parsing key");
                k.and_then(|k| {
                    from_utf8(kv.value())
                        .context("Converting value")
                        .map(|v| (k, str::to_string(v)))
                })
                .map_err(AnyErr::from)
            })
            .collect();

        messages
    }
}

async fn hello_world_tx(tx: &Transaction, h: &str, w: &str) -> FdbResult<()> {
    tx.set(b"hello", b"world");

    Ok(())
}

async fn hello_world() -> FdbResult<()> {
    let db = Database::default()?;

    // write a value
    // let tx = db.create_tx()?;
    // tx.set(b"hello", b"world"); // errors will be returned in the future result
    // tx.commit().await?;

    let (h, w) = ("hello", "world");

    db.transact_boxed_local(
        (h, w),
        move |tx, (h, w)| hello_world_tx(tx, h, w).boxed_local(),
        ChatOpts,
    )
    .await?;

    // read a value
    let tx = db.create_trx()?;
    let maybe_value = tx.get(b"hello", false).await?;
    let value = maybe_value.unwrap(); // unwrap the option

    assert_eq!(b"world", &value.as_ref());

    Ok(())
}

async fn send_message(s: &str) {
    println!("Got line {}", s);
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    username: String,

    #[clap(short, long)]
    room: String,
}

async fn main_loop() -> anyhow::Result<()> {
    let args = Args::parse();

    let db = Arc::new(foundationdb::Database::default()?);

    let session = Session::init(db, args.room, args.username).await?;

    let mut input = Input::new();

    loop {
        let sent = async {
            let line = input.next().await;
            match line {
                Ok(l) => {
                    send_message(&l).await;
                    Ok(())
                }
                Err(e) => Err(e),
            }
        };

        let mut next_line = Box::pin(sent.fuse());
        select! {
            found = next_line => found?,
        }
    }
}

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let network = unsafe { foundationdb::boot() };

    let result = main_loop().await;

    drop(network);

    result
}

// fn main() {
//     tokio::runtime::Builder::hello_world().await?;
// }
