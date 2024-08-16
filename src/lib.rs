/*
Copyright (c) 2021 Florian Brucker (www.florianbrucker.de)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

/*!
A streaming API for the [`roux`] Reddit client.

Reddit's API does not provide "firehose"-style streaming of new posts and
comments. Instead, the endpoints for retrieving the latest posts and comments
have to be polled regularly. This crate automates that task and provides streams
for a subreddit's posts (submissions) and comments.

See [`stream_submissions`] and [`stream_comments`] for
details.

# Logging

This module uses the logging infrastructure provided by the [`log`] crate.
*/

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use futures::channel::mpsc;
use futures::Stream;
use futures::{Sink, SinkExt};
use log::{debug, warn};

use roux::Reddit;
use roux::{
    comment::CommentData,
    response::{BasicThing, Listing},
    submission::SubmissionData,
    util::RouxError,
    Subreddit,
};
use std::error::Error;
use std::fmt::Display;
use std::marker::Unpin;
use std::{collections::HashSet, time::Duration};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::error::Elapsed;
use tokio::time::sleep;
use tokio_retry::RetryIf;

/**
The [`roux`] APIs for submissions and comments are slightly different. We use
the [`Puller`] trait as the common interface to which we then adapt those APIs.
This allows us to implement our core logic (e.g. retries and duplicate
filtering) once without caring about the differences between submissions and
comments. In addition, this makes it easier to test the core logic because
we can provide a mock implementation.
*/
#[async_trait]
trait Puller<Data, E> {
    // The "real" implementations of this function (for pulling
    // submissions and comments from Reddit) would not need `self` to
    // be `mut` here (because there the state change happens externally,
    // i.e. within Reddit). However, writing good tests is much easier
    // if `self` is mutable here.
    async fn pull(&mut self) -> Result<BasicThing<Listing<BasicThing<Data>>>, E>;
    fn get_id(&self, data: &Data) -> String;
    fn get_items_name(&self) -> String;
    fn get_source_name(&self) -> String;
    async fn update_reddit_client(&mut self) -> Result<(), E>;
}

struct SubredditPuller {
    last_login: DateTime<Utc>,
    reddit: Option<Reddit>,
    subreddit: Subreddit,
}

// How many items to fetch per request
const LIMIT: u32 = 100;

#[async_trait]
impl Puller<SubmissionData, RouxError> for SubredditPuller {
    async fn pull(&mut self) -> Result<BasicThing<Listing<BasicThing<SubmissionData>>>, RouxError> {
        if let Err(e) =
            <SubredditPuller as Puller<SubmissionData, RouxError>>::update_reddit_client(self).await
        {
            debug!("Failed to update Reddit client: {}", e);
        }

        self.subreddit.latest(LIMIT, None).await
    }

    fn get_id(&self, data: &SubmissionData) -> String {
        data.id.clone()
    }

    fn get_items_name(&self) -> String {
        "submissions".to_owned()
    }

    fn get_source_name(&self) -> String {
        format!("r/{}", self.subreddit.name)
    }

    async fn update_reddit_client(&mut self) -> Result<(), RouxError> {
        let login_time_diff = Utc::now() - self.last_login;

        if login_time_diff.num_seconds() > 86400 {
            debug!("Session expired. Updating reddit client");
            if let Some(ref reddit) = self.reddit {
                let client = reddit.clone().login().await?.client;

                self.last_login = Utc::now();
                self.subreddit = Subreddit::new_oauth(self.subreddit.name.as_str(), &client);
            }
        } else {
            debug!("Session not expired. Skipping login");
        }

        Ok(())
    }
}

#[async_trait]
impl Puller<CommentData, RouxError> for SubredditPuller {
    async fn pull(&mut self) -> Result<BasicThing<Listing<BasicThing<CommentData>>>, RouxError> {
        self.subreddit.latest_comments(None, Some(LIMIT)).await
    }

    fn get_id(&self, data: &CommentData) -> String {
        data.id.as_ref().cloned().unwrap()
    }

    fn get_items_name(&self) -> String {
        "comments".to_owned()
    }

    fn get_source_name(&self) -> String {
        format!("r/{}", self.subreddit.name)
    }

    async fn update_reddit_client(&mut self) -> Result<(), RouxError> {
        let login_time_diff = Utc::now() - self.last_login;

        if login_time_diff.num_seconds() > 86400 {
            debug!("Session expired. Updating reddit client");
            if let Some(ref reddit) = self.reddit {
                let client = reddit.clone().login().await?.client;

                self.subreddit = Subreddit::new_oauth(self.subreddit.name.as_str(), &client);
            }
        } else {
            debug!(
                "Session not expired ({}). Skipping login",
                self.last_login.to_string()
            );
        }

        Ok(())
    }
}

/**
Error that occurs when pulling new data from Reddit failed.
 */
#[derive(Debug, PartialEq)]
pub enum StreamError<E> {
    /**
    Returned when pulling new data timed out.
     */
    TimeoutError(Elapsed),

    /**
    Returned when [`roux`] reported an error while pulling new data.
    */
    SourceError(E),
}

impl<E> Display for StreamError<E>
where
    E: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::TimeoutError(err) => err.fmt(f),
            StreamError::SourceError(err) => err.fmt(f),
        }
    }
}

impl<E> Error for StreamError<E> where E: std::fmt::Debug + Display {}

/**
Pull new items from Reddit and push them into a sink.

This function contains the core of the streaming logic. It performs the
following steps in an endless loop:

1. Pull the latest items (submissions or comments) from Reddit, retrying
   that operation if necessary according to `retry_strategy`. If
   `timeout` is given and pulling the items from Reddit takes longer
   then abort and yield an error (see step 3).
2. Filter out already seen items using their ID.
3. Push the new items (or an error if pulling failed) into `sink`.
4. Sleep for `sleep_time`.
*/
async fn pull_into_sink<S, R, Data, E>(
    puller: &mut (dyn Puller<Data, E> + Send + Sync),
    sleep_time: Duration,
    retry_strategy: R,
    timeout: Option<Duration>,
    mut sink: S,
) -> Result<(), S::Error>
where
    S: Sink<Result<Data, StreamError<E>>> + Unpin,
    R: IntoIterator<Item = Duration> + Clone,
    E: Error,
{
    let items_name = puller.get_items_name();
    let source_name = puller.get_source_name();
    let mut seen_ids: HashSet<String> = HashSet::new();

    /*
    Because `puller.pull` takes a mutable reference we need wrap it in
    a mutex to be able to pass it as a callback to `RetryIf::spawn`.
     */
    let puller_mutex = Mutex::new(puller);

    loop {
        debug!("Fetching latest {} from {}", items_name, source_name);
        let latest = RetryIf::spawn(
            retry_strategy.clone(),
            || async {
                let mut puller = puller_mutex.lock().await;

                // TODO: There is probably a nicer way to write those matches

                if let Some(timeout_duration) = timeout {
                    let timeout_result =
                        tokio::time::timeout(timeout_duration, puller.pull()).await;
                    match timeout_result {
                        Err(timeout_err) => Err::<BasicThing<Listing<BasicThing<Data>>>, _>(
                            StreamError::TimeoutError(timeout_err),
                        ),
                        Ok(timeout_ok) => match timeout_ok {
                            Err(puller_err) => Err(StreamError::SourceError(puller_err)),
                            Ok(pull_ok) => Ok(pull_ok),
                        },
                    }
                } else {
                    match puller.pull().await {
                        Err(puller_err) => Err(StreamError::SourceError(puller_err)),
                        Ok(pull_ok) => Ok(pull_ok),
                    }
                }
            },
            |error: &StreamError<E>| {
                debug!(
                    "Error while fetching the latest {} from {}: {}",
                    items_name, source_name, error,
                );
                true
            },
        )
        .await;
        match latest {
            Ok(latest_items) => {
                let latest_items = latest_items.data.children.into_iter().map(|item| item.data);
                let mut latest_ids: HashSet<String> = HashSet::new();

                let mut num_new = 0;
                let puller = puller_mutex.lock().await;
                for item in latest_items {
                    let id = puller.get_id(&item);
                    latest_ids.insert(id.clone());
                    if !seen_ids.contains(&id) {
                        num_new += 1;
                        sink.send(Ok(item)).await?;
                    }
                }

                debug!(
                    "Got {} new {} for {} (out of {})",
                    num_new, items_name, source_name, LIMIT
                );
                if num_new == latest_ids.len() && !seen_ids.is_empty() {
                    warn!(
                        "All received {} for {} were new, try a shorter sleep_time",
                        items_name, source_name
                    );
                }

                seen_ids = latest_ids;
            }
            Err(error) => {
                // Forward the error through the stream
                warn!(
                    "Error while fetching the latest {} from {}: {}",
                    items_name, source_name, error,
                );
                sink.send(Err(error)).await?;
            }
        }

        sleep(sleep_time).await;
    }
}

/**
Spawn a task that pulls items and puts them into a stream.

Depending on `T`, this function will either stream submissions or comments.
*/
fn stream_items<R, I, T>(
    subreddit: &Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
    timeout: Option<Duration>,
    reddit: Option<Reddit>,
) -> (
    impl Stream<Item = Result<T, StreamError<RouxError>>>,
    JoinHandle<Result<(), mpsc::SendError>>,
)
where
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone + Send + Sync + 'static,
    I: Iterator<Item = Duration> + Send + Sync + 'static,
    SubredditPuller: Puller<T, RouxError>,
    T: Send + 'static,
{
    let (sink, stream) = mpsc::unbounded();
    // We need an owned instance (or at least statically bound
    // reference) for tokio::spawn. Since Subreddit isn't Clone,
    // we simply create a new instance.

    let subreddit = Subreddit::new(subreddit.name.as_str());

    let mut pull = SubredditPuller {
        subreddit,
        reddit,
        last_login: Utc.timestamp_opt(0, 0).unwrap(),
    };

    let join_handle = tokio::spawn(async move {
        pull_into_sink(&mut pull, sleep_time, retry_strategy, timeout, sink).await
    });
    (stream, join_handle)
}

/**
Stream new submissions in a subreddit.

Creates a separate tokio task that regularly polls the subreddit for new
submissions. Previously unseen submissions are sent into the returned
stream.

Returns a tuple `(stream, join_handle)` where `stream` is the
[`Stream`](futures::Stream) from which the submissions can be read, and
`join_handle` is the [`JoinHandle`](tokio::task::JoinHandle) for the
polling task.

`sleep_time` controls the interval between calls to the Reddit API, and
depends on how much traffic the subreddit has. Each call fetches the 100
latest items (the maximum number allowed by Reddit). A warning is logged
if none of those items has been seen in the previous call: this indicates
a potential miss of new content and suggests that a smaller `sleep_time`
should be chosen. Enable debug logging for more statistics.

If `timeout` is not `None` then calls to the Reddit API that take longer
than `timeout` are aborted with a [`StreamError::TimeoutError`].

If an error occurs while fetching the latest submissions from Reddit then
fetching is retried according to `retry_strategy` (see [`tokio_retry`] for
details). If one of the retries succeeds then normal operation is resumed.
If `retry_strategy` is finite and the last retry fails then its error is
sent into the stream, afterwards normal operation is resumed.

The spawned task runs indefinitely unless an error is encountered when
sending data into the stream (for example because the receiver is dropped).
In that case the task stops and the error is returned via `join_handle`.

See also [`stream_comments`].


# Example

The following example prints new submissions to
[r/AskReddit](https://reddit.com/r/AskReddit) in an endless loop.

```
use futures::StreamExt;
use roux::Subreddit;
use roux_stream::stream_submissions;
use std::time::Duration;
use tokio_retry::strategy::ExponentialBackoff;

#[tokio::main]
async fn main() {
    let subreddit = Subreddit::new("AskReddit");

    // How often to retry when pulling the data from Reddit fails and
    // how long to wait between retries. See the docs of `tokio_retry`
    // for details.
    let retry_strategy = ExponentialBackoff::from_millis(5).factor(100).take(3);

    let (mut stream, join_handle) = stream_submissions(
        &subreddit,
        Duration::from_secs(60),
        retry_strategy,
        Some(Duration::from_secs(10)),
    );

    while let Some(submission) = stream.next().await {
        // `submission` is an `Err` if getting the latest submissions
        // from Reddit failed even after retrying.
        let submission = submission.unwrap();
        println!("\"{}\" by {}", submission.title, submission.author);
        # // An endless loop doesn't work well with doctests, so in that
        # // case we abort the task and exit the loop directly.
        # join_handle.abort();
        # break;
    }
    # // Aborting the task will make the join handle return an error. Let's
    # // make sure it's the right one.
    # let join_result = join_handle.await;
    # assert!(join_result.is_err());
    # assert!(join_result.err().unwrap().is_cancelled());
    # // Now we need to make sure that the remaining code in the example
    # // still works, so we create a fake `join_handle` for it to work
    # // with.
    # let join_handle = async { Some(Some(())) };

    // In case there was an error sending the submissions through the
    // stream, `join_handle` will report it.
    join_handle.await.unwrap().unwrap();
}
```
*/
pub fn stream_submissions<R, I>(
    subreddit: &Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
    timeout: Option<Duration>,
    reddit: Option<Reddit>,
) -> (
    impl Stream<Item = Result<SubmissionData, StreamError<RouxError>>>,
    JoinHandle<Result<(), mpsc::SendError>>,
)
where
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone + Send + Sync + 'static,
    I: Iterator<Item = Duration> + Send + Sync + 'static,
{
    stream_items(subreddit, sleep_time, retry_strategy, timeout, reddit)
}

/**
Stream new comments in a subreddit.

Creates a separate tokio task that regularly polls the subreddit for new
comments. Previously unseen comments are sent into the returned
stream.

Returns a tuple `(stream, join_handle)` where `stream` is the
[`Stream`](futures::Stream) from which the comments can be read, and
`join_handle` is the [`JoinHandle`](tokio::task::JoinHandle) for the
polling task.

`sleep_time` controls the interval between calls to the Reddit API, and
depends on how much traffic the subreddit has. Each call fetches the 100
latest items (the maximum number allowed by Reddit). A warning is logged
if none of those items has been seen in the previous call: this indicates
a potential miss of new content and suggests that a smaller `sleep_time`
should be chosen. Enable debug logging for more statistics.

If `timeout` is not `None` then calls to the Reddit API that take longer
than `timeout` are aborted with a [`StreamError::TimeoutError`].

If an error occurs while fetching the latest comments from Reddit then
fetching is retried according to `retry_strategy` (see [`tokio_retry`] for
details). If one of the retries succeeds then normal operation is resumed.
If `retry_strategy` is finite and the last retry fails then its error is
sent into the stream, afterwards normal operation is resumed.

The spawned task runs indefinitely unless an error is encountered when
sending data into the stream (for example because the receiver is dropped).
In that case the task stops and the error is returned via `join_handle`.

See also [`stream_submissions`].


# Example

The following example prints new comments to
[r/AskReddit](https://reddit.com/r/AskReddit) in an endless loop.

```
use futures::StreamExt;
use roux::Subreddit;
use roux_stream::stream_comments;
use std::time::Duration;
use tokio_retry::strategy::ExponentialBackoff;


#[tokio::main]
async fn main() {
    let subreddit = Subreddit::new("AskReddit");

    // How often to retry when pulling the data from Reddit fails and
    // how long to wait between retries. See the docs of `tokio_retry`
    // for details.
    let retry_strategy = ExponentialBackoff::from_millis(5).factor(100).take(3);

    let (mut stream, join_handle) = stream_comments(
        &subreddit,
        Duration::from_secs(10),
        retry_strategy,
        Some(Duration::from_secs(10)),
    );

    while let Some(comment) = stream.next().await {
        // `comment` is an `Err` if getting the latest comments
        // from Reddit failed even after retrying.
        let comment = comment.unwrap();
        println!(
            "{}{} (by u/{})",
            comment.link_url.unwrap(),
            comment.id.unwrap(),
            comment.author.unwrap()
        );
        # // An endless loop doesn't work well with doctests, so in that
        # // case we abort the task and exit the loop directly.
        # join_handle.abort();
        # break;
    }
    # // Aborting the task will make the join handle return an error. Let's
    # // make sure it's the right one.
    # let join_result = join_handle.await;
    # assert!(join_result.is_err());
    # assert!(join_result.err().unwrap().is_cancelled());
    # // Now we need to make sure that the remaining code in the example
    # // still works, so we create a fake `join_handle` for it to work
    # // with.
    # let join_handle = async { Some(Some(())) };

    // In case there was an error sending the submissions through the
    // stream, `join_handle` will report it.
    join_handle.await.unwrap().unwrap();
}
```
*/
pub fn stream_comments<R, I>(
    subreddit: &Subreddit,
    sleep_time: Duration,
    retry_strategy: R,
    timeout: Option<Duration>,
    reddit: Option<Reddit>,
) -> (
    impl Stream<Item = Result<CommentData, StreamError<RouxError>>>,
    JoinHandle<Result<(), mpsc::SendError>>,
)
where
    R: IntoIterator<IntoIter = I, Item = Duration> + Clone + Send + Sync + 'static,
    I: Iterator<Item = Duration> + Send + Sync + 'static,
{
    stream_items(subreddit, sleep_time, retry_strategy, timeout, reddit)
}
