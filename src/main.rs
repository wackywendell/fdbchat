use std::collections::VecDeque;
use std::fmt::Display;
use std::str::from_utf8;

use anyhow::Context;
use async_std::io;
use clap::Parser;
use foundationdb::future::{FdbKeyValue, FdbValues};
use foundationdb::tuple::{pack, unpack, Subspace};
use foundationdb::{Database, FdbError, FdbResult, KeySelector, RangeOption, Transaction};
use futures::future::select;
use futures::Future;
use futures::{future::Either, future::FutureExt, pin_mut, stream::StreamExt};
use signal_hook::consts::signal::*;
use signal_hook_async_std::Signals;
use uuid::Uuid;

type DateTime = chrono::DateTime<chrono::Utc>;

/// A wrapper error for FoundationDB errors OR any other error.
///
/// This error implements foundationdb::TransactError so that FoundationDB
/// errors can be retried and other errors can be passed through.
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
    db: foundationdb::Database,
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

    async fn init_tx(tx: &Transaction, room: &str, username: &str, uuid: Uuid) -> AnyResult<()> {
        let key = Session::user_key(room, username);
        let val = tx.get(&pack(&key), false).await?;

        if let Some(_taken_id) = val {
            return Err(anyhow::format_err!(
                "Username {} already taken in room {}!",
                username,
                room
            )
            .into());
        };

        tx.set(&pack(&key), &pack(&uuid));

        Ok(())
    }

    async fn init(db: Database, room: String, username: String) -> AnyResult<Self> {
        let id = Uuid::new_v4();

        db.transact_boxed_local(
            (room.as_ref(), username.as_ref()),
            move |tx: &Transaction, (room, username)| {
                Session::init_tx(tx, room, username, id).boxed_local()
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

    pub async fn clear(db: &Database, room: &str) -> FdbResult<()> {
        let space = Subspace::from(&("rooms", &room));

        db.transact_boxed_local(
            space,
            |tx, space| {
                tx.clear_subspace_range(space);
                futures::future::ready(Ok(())).boxed_local()
            },
            CHAT_OPTS,
        )
        .await
    }

    async fn leave_tx(tx: &Transaction, id: Uuid, room: &str, username: &str) -> AnyResult<()> {
        let key = ("rooms", room, "users", username);
        let keyp = pack(&key);
        let val = tx.get(&keyp, true).await?;

        let dbid: Uuid = match val {
            Some(v) => unpack(&v).map_err(anyhow::Error::from)?,
            None => return Err(anyhow::format_err!("Key is unset somehow").into()),
        };

        if dbid != id {
            return Err(anyhow::format_err!("Unexpected ID").into());
        }

        tx.clear(&keyp);

        Ok(())
    }

    /// Leave the chat room and close the session.
    pub async fn leave(self) -> AnyResult<()> {
        let Session {
            db,
            room,
            username,
            id,
        } = self;
        let id = match id {
            None => return Ok(()),
            Some(id) => id,
        };
        db.transact_boxed_local(
            (room, username, id),
            |tx: &Transaction, (room, username, id)| {
                Session::leave_tx(tx, *id, room, username).boxed_local()
            },
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
                Ok(v) => {
                    log::info!("MessageIter: Got {} messages", v.len());
                    break v;
                }
                Err(w) => {
                    log::info!("MessageIter: Waiting");
                    w.await?
                }
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

    #[clap(short, long, parse(from_occurrences))]
    debug: usize,

    #[clap(long)]
    clear: bool,
}

async fn message_print_loop(session: &Session) -> anyhow::Result<()> {
    let mut iter = MessageIter::new(session, None);

    loop {
        let (dt, msg) = iter.next().await?;
        println!("{}: {}", dt, msg);
    }
}

async fn send_loop(session: &Session) -> anyhow::Result<()> {
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

async fn signal_loop() -> anyhow::Result<()> {
    let mut signals = Signals::new(&[SIGHUP, SIGTERM, SIGINT, SIGQUIT])?;
    let handle = signals.handle();

    while let Some(signal) = signals.next().await {
        match signal {
            SIGHUP => {
                log::warn!("Received SIGHUP, exiting.");
                break;
            }
            SIGTERM => {
                log::warn!("Received SIGTERM, exiting.");
                break;
            }
            SIGINT => {
                log::info!("Received SIGINT, exiting.");
                break;
            }
            SIGQUIT => {
                log::warn!("Received SIGQUIT, exiting.");
                break;
            }
            _ => unreachable!(),
        }
    }

    handle.close();
    Ok(())
}

async fn main_loop() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut builder = env_logger::Builder::from_env("LOGLEVEL");
    match args.debug {
        0 => {}
        1 => {
            builder.filter_level(log::LevelFilter::Info);
        }
        _ => {
            builder.filter_level(log::LevelFilter::Debug);
        }
    }
    builder.init();

    let db = foundationdb::Database::default()?;
    if args.clear {
        Session::clear(&db, &args.room).await?;
    }

    let session = Session::init(db, args.room, args.username).await?;

    {
        let sender = send_loop(&session);
        let receiver = message_print_loop(&session);
        let signals = signal_loop();
        pin_mut!(sender);
        pin_mut!(receiver);
        pin_mut!(signals);

        match select(signals, select(sender, receiver)).await {
            // Got a signal, so we're done
            Either::Left((signal_result, _other_future)) => signal_result?,
            // Either sender or receiver returned, so we take the first of the
            // two and short-circuit on the error
            Either::Right((inner, _other_future)) => inner.factor_first().0?,
        }
    };

    session.leave().await?;

    Ok(())
}

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let network = unsafe { foundationdb::boot() };

    let result = main_loop().await;

    drop(network);

    result
}
