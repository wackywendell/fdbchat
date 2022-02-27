# FdbChat: A README

This is a basic chat server built on FoundationDB.

It supports multiple rooms and multiple users.

When users join, all messages in the room are printed to stdout. Messages can be sent via stdin, and messages from both self and others will be printed to stdout.

## Example Usage

Shell inputs are marked with `❯`, and stdin lines are marked with `<!>` at the end (to reproduce, skip the `<!>`).

### Basic Back and Forth

#### Terminal 1: Alice

Alice joins first (and uses `--clear` to clear history _for everyone_) and writes a few messages before anyone else shows up:

```
❯ cargo build --release
❯ ./target/release/fdbchat --room exampleroom --username alice --clear
Hello from alice! <!>
2022-02-27 20:37:57.034 UTC: Hello from alice!
No one is here yet! <!>
2022-02-27 20:38:02.369 UTC: No one is here yet!
Bob should join now... <!>
2022-02-27 20:38:13.465 UTC: Bob should join now...
Bob, are you here yet? <!>
2022-02-27 20:38:19.743 UTC: Bob, are you here yet?
2022-02-27 20:38:45.695 UTC: Yes, I'm here!
2022-02-27 20:38:51.393 UTC: Hello, Alice!
Good to hear from you, Bob! <!>
2022-02-27 20:38:56.324 UTC: Good to hear from you, Bob!
```

#### Terminal 2: Bob

Bob shows up next after a few messages from Alice:

```
❯ ./target/release/fdbchat --room exampleroom --username bob         
2022-02-27 20:37:57.034 UTC: Hello from alice!
2022-02-27 20:38:02.369 UTC: No one is here yet!
2022-02-27 20:38:13.465 UTC: Bob should join now...
2022-02-27 20:38:19.743 UTC: Bob, are you here yet?
Yes, I'm here! <!>
2022-02-27 20:38:45.695 UTC: Yes, I'm here!
Hello, Alice! <!>
2022-02-27 20:38:51.393 UTC: Hello, Alice!
2022-02-27 20:38:56.324 UTC: Good to hear from you, Bob!
```

#### Terminal 3: Charlie

Charlie shows up after all but the last message, and turns on debug logging.

The buffer in `MessageIter` fetches 3 messages at a time, and then uses a FoundationDB Watch ([Rust method](https://docs.rs/foundationdb/0.5.0/foundationdb/struct.Transaction.html#method.watch), [FoundationDB wiki link](https://github.com/apple/foundationdb/wiki/An-Overview-how-Watches-Work)) to wait for new messages: 

```
❯ ./target/release/fdbchat --room exampleroom --username charlie -dd
[2022-02-27T20:45:55Z INFO  fdbchat] MessageIter: Got 3 messages
2022-02-27 20:37:57.034 UTC: Hello from alice!
2022-02-27 20:38:02.369 UTC: No one is here yet!
2022-02-27 20:38:13.465 UTC: Bob should join now...
[2022-02-27T20:45:55Z INFO  fdbchat] MessageIter: Got 3 messages
2022-02-27 20:38:19.743 UTC: Bob, are you here yet?
2022-02-27 20:38:45.695 UTC: Yes, I'm here!
2022-02-27 20:38:51.393 UTC: Hello, Alice!
[2022-02-27T20:38:51Z INFO  fdbchat] MessageIter: Waiting
[2022-02-27T20:38:56Z INFO  fdbchat] MessageIter: Got 1 messages
2022-02-27 20:38:56.324 UTC: Good to hear from you, Bob!
[2022-02-27T20:38:56Z INFO  fdbchat] MessageIter: Waiting
```