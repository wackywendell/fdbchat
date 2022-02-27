use std::collections::VecDeque;
use std::fmt::Display;
use std::str::from_utf8;
use std::sync::Arc;

use anyhow::Context;
use async_std::io;
use clap::Parser;
use foundationdb::future::{FdbKeyValue, FdbValues};
use foundationdb::tuple::{pack, unpack, Subspace};
use foundationdb::{Database, FdbError, FdbResult, KeySelector, RangeOption, Transaction};
use futures::future::select;
use futures::Future;
use futures::{
    future::FutureExt, // for `.fuse()`
    pin_mut,
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

const CHAT_OPTS: foundationdb::TransactOption = foundationdb::TransactOption {
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

    fn date_string(dt: DateTime) -> String {
        dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
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
            CHAT_OPTS,
        )
        .await?;

        Ok(Session {
            db,
            room,
            username,
            id: Some(id),
        })
    }

    pub async fn clear(&self) -> FdbResult<()> {
        let space = Subspace::from(&("rooms", &self.room, "messages"));

        self.db
            .transact_boxed_local(
                space,
                |tx, space| {
                    tx.clear_subspace_range(space);
                    futures::future::ready(Ok(())).boxed_local()
                },
                CHAT_OPTS,
            )
            .await
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
            CHAT_OPTS,
        )
        .await
    }

    fn message_key(room: &str, dt: DateTime) -> (&str, &str, &str, String) {
        ("rooms", room, "messages", Session::date_string(dt))
    }

    fn message_recent_key(room: &str) -> (&str, &str, &str) {
        ("rooms", room, "most_recent_message")
    }

    pub async fn write(&self, dt: DateTime, message: &str) -> AnyResult<()> {
        let message_key = Session::message_key(&self.room, dt);
        let dt_key = message_key.3.as_ref();
        let recent_key = Session::message_recent_key(&self.room);

        self.db
            .transact_boxed_local(
                (pack(&message_key), pack(&recent_key), dt_key, message),
                |tx, (message_key, recent_key, dt_key, message)| {
                    async move {
                        tx.set(message_key, message.as_bytes());
                        tx.set(recent_key, dt_key);
                        Ok(())
                    }
                    .boxed_local()
                },
                CHAT_OPTS,
            )
            .await
    }

    pub async fn read_all_and_watch(
        &self,
        last: Option<DateTime>,
    ) -> AnyResult<(Vec<(DateTime, String)>, impl Future<Output = FdbResult<()>>)> {
        let space = Subspace::from(&("rooms", &self.room, "messages"));
        let recent_key = Session::message_recent_key(&self.room);

        let r: RangeOption = match last {
            None => RangeOption::from(&space),
            Some(dt) => {
                let (_begin, end) = space.range();
                let last_key = pack(&Session::message_key(&self.room, dt));
                let ks = KeySelector::first_greater_than(last_key);
                RangeOption::from((ks, KeySelector::first_greater_or_equal(end)))
            }
        };

        let (kvs, watch) = self
            .db
            .transact_boxed_local::<_, _, _, FdbError>(
                (r, pack(&recent_key)),
                |tx, (r, recent_key)| {
                    async move {
                        let kvs = tx.get_range(r, 1, true).await?;

                        let watch = tx.watch(recent_key);

                        Ok((kvs, watch))
                    }
                    .boxed_local()
                },
                CHAT_OPTS,
            )
            .await?;

        let messages: Vec<(DateTime, String)> = kvs
            .iter()
            .map(Session::parse_kv)
            .collect::<AnyResult<_>>()?;

        Ok((messages, watch))
    }

    pub async fn read_all(&self, last: Option<DateTime>) -> AnyResult<Vec<(DateTime, String)>> {
        let space = Subspace::from(&("rooms", &self.room, "messages"));

        let r: RangeOption = match last {
            None => RangeOption::from(&space),
            Some(dt) => {
                let (_begin, end) = space.range();
                let last_key = pack(&Session::message_key(&self.room, dt));
                let ks = KeySelector::first_greater_than(last_key);
                RangeOption::from((ks, KeySelector::first_greater_or_equal(end)))
            }
        };

        let kvs = self
            .db
            .transact_boxed_local::<_, _, _, FdbError>(
                r,
                |tx, r| tx.get_range(r, 1, true).boxed_local(),
                CHAT_OPTS,
            )
            .await?;

        kvs.iter().map(Session::parse_kv).collect::<AnyResult<_>>()
    }

    /// messages_or_watch returns a list of messages, or if none are available, a watch that will
    /// trigger when at least one message is available.
    ///
    /// last: If None, start with the first message; otherwise, start after this message.
    /// limit: if None, returns all waiting messages; otherwise, returns up to limit messages.
    pub async fn messages_or_watch(
        &self,
        last: Option<DateTime>,
        limit: Option<usize>,
    ) -> AnyResult<Result<Vec<(DateTime, String)>, impl Future<Output = FdbResult<()>>>> {
        let space = Subspace::from(&("rooms", &self.room, "messages"));
        let recent_key = Session::message_recent_key(&self.room);

        let mut r: RangeOption = match last {
            None => RangeOption::from(&space),
            Some(dt) => {
                let (_begin, end) = space.range();
                let last_key = pack(&Session::message_key(&self.room, dt));
                let ks = KeySelector::first_greater_than(last_key);
                RangeOption::from((ks, KeySelector::first_greater_or_equal(end)))
            }
        };

        r.limit = limit;

        let kvs: Result<FdbValues, _> = self
            .db
            .transact_boxed_local::<_, _, _, FdbError>(
                (&r, pack(&recent_key)),
                |tx, (r, recent_key)| {
                    async move {
                        let kvs = tx.get_range(r, 1, false).await;
                        match kvs {
                            Err(e) => Err(e),
                            Ok(kv) if kv.is_empty() => Ok(Err(tx.watch(recent_key))),
                            Ok(kv) => Ok(Ok(kv)),
                        }
                    }
                    .boxed_local()
                },
                CHAT_OPTS,
            )
            .await?;

        match kvs {
            Ok(kvs) => kvs
                .iter()
                .map(Session::parse_kv)
                .collect::<AnyResult<Vec<_>>>()
                .map(Ok),
            Err(w) => Ok(Err(w)),
        }
    }

    fn parse_kv(kv: &FdbKeyValue) -> AnyResult<(DateTime, String)> {
        let (_, _, _, kdt): (String, String, String, String) =
            unpack(kv.key()).context("Unpacking")?;
        let fixed_dt = chrono::DateTime::parse_from_rfc3339(&kdt).context("Parsing date")?;
        let dt = DateTime::from(fixed_dt);

        let msg = str::to_string(from_utf8(kv.value()).context("Parsing date")?);

        Ok((dt, msg))
    }
}

pub struct MessageIter<'a> {
    session: &'a Session,
    last: Option<DateTime>,
    waiting: VecDeque<(DateTime, String)>,
    // watch: Option<Box<impl Future<Output = AnyResult<()>>>>,
}

impl<'a> MessageIter<'a> {
    pub fn new(session: &'a Session, last: Option<DateTime>) -> Self {
        MessageIter {
            session,
            last,
            waiting: VecDeque::new(),
        }
    }

    pub async fn next(&mut self) -> AnyResult<(DateTime, String)> {
        if let Some(dm) = self.waiting.pop_front() {
            return Ok(dm);
        }

        // None left in the past; let's see if any are waiting, and wait if they are
        let messages = loop {
            let msg_res = self.session.messages_or_watch(self.last, Some(3)).await?;
            match msg_res {
                Ok(v) => break v,
                Err(w) => w.await?,
            }
        };
        self.waiting.extend(messages);

        let (last_dt, _) = self.waiting.back().expect("Messages expected after watch");
        self.last = Some(*last_dt);

        let msg = self
            .waiting
            .pop_front()
            .expect("Really expected a front message after waiting for watch and extending");

        Ok(msg)
    }
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    username: String,

    #[clap(short, long)]
    room: String,

    #[clap(long)]
    clear: bool,
}

async fn message_print_loop(session: &Session) -> AnyResult<()> {
    let mut iter = MessageIter::new(session, None);

    loop {
        let (dt, msg) = iter.next().await?;
        println!("{}: {}", dt, msg);
    }
}

async fn send_loop(session: &Session) -> AnyResult<()> {
    let mut input = Input::new();

    loop {
        let line = input.next().await.context("Failed getting input line")?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let now = chrono::Utc::now();
        session.write(now, line).await?;
    }
}

async fn main_loop() -> AnyResult<()> {
    let args = Args::parse();

    let db = Arc::new(foundationdb::Database::default()?);

    let session = Session::init(db, args.room, args.username).await?;
    if args.clear {
        session.clear().await?;
    }

    {
        let sender = send_loop(&session);
        let receiver = message_print_loop(&session);
        pin_mut!(sender);
        pin_mut!(receiver);

        select(sender, receiver).await.factor_first().0?;
    };

    session.leave().await?;

    Ok(())
}

#[async_std::main]
async fn main() -> AnyResult<()> {
    let network = unsafe { foundationdb::boot() };

    let result = main_loop().await;

    drop(network);

    result
}

// fn main() {
//     tokio::runtime::Builder::hello_world().await?;
// }
