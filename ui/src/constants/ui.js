export const APP_TAB = Object.freeze({
  OVERVIEW: "Overview",
  TOPICS: "Topics",
  PRODUCERS: "Producers",
  CONSUMERS: "Consumers",
  MESSAGES: "Messages",
  DEAD_LETTERS: "Dead Letters",
  WORKFLOWS: "Workflows (v2)"
});

export const TABS = Object.freeze(Object.values(APP_TAB));

export const PROMISE_STATUS = Object.freeze({
  FULFILLED: "fulfilled"
});

export const HEALTH_STATUS = Object.freeze({
  OK: "ok",
  DEGRADED: "degraded",
  DISCONNECTED: "disconnected",
  UNKNOWN: "unknown"
});

export const CONNECTION_STATUS_LABEL = Object.freeze({
  [HEALTH_STATUS.OK]: "Connected",
  [HEALTH_STATUS.DEGRADED]: "Degraded",
  [HEALTH_STATUS.DISCONNECTED]: "Disconnected",
  DEFAULT: "Connecting"
});

export const COMMON_TEXT = Object.freeze({
  EMPTY: "",
  DASH: "-",
  UNKNOWN: "unknown",
  NOT_AVAILABLE: "n/a",
  DEV: "dev",
  LOADING: "loading...",
  REFRESHING: "refreshing",
  NO_TOPICS_AVAILABLE: "No topics available"
});

export const DEFAULTS = Object.freeze({
  GROUP: "bench",
  UPDATED_AT: COMMON_TEXT.DASH,
  VERSION: Object.freeze({
    version: COMMON_TEXT.UNKNOWN,
    commit: COMMON_TEXT.UNKNOWN,
    wal_enabled: false
  }),
  CONFIG: Object.freeze({
    addr: COMMON_TEXT.EMPTY,
    wal_path: COMMON_TEXT.EMPTY,
    access_log: false,
    engine_store: COMMON_TEXT.UNKNOWN,
    engine_wal: COMMON_TEXT.UNKNOWN,
    artifacts_dir: COMMON_TEXT.EMPTY,
    log_level: COMMON_TEXT.UNKNOWN,
    log_format: COMMON_TEXT.UNKNOWN,
    max_partition_bytes: 0,
    max_partition_msgs: 0,
    max_inflight: 0,
    wal_sync_interval: COMMON_TEXT.EMPTY,
    wal_buffer_bytes: 0
  }),
  TOPIC_PARTITIONS: "1",
  CONSUMER_OWNER: "debug-ui",
  CONSUMER_LEASE_MS: "10000",
  CONSUMER_NACK_REASON: "debug reject",
  WORKFLOW_CANCEL_REASON: "dashboard cancel"
});

export const UI_LIMITS = Object.freeze({
  REFRESH_INTERVAL_MS: 4000,
  RUN_LIST_LIMIT: 10,
  WORKFLOW_PREVIEW_RUNS: 6,
  TOPIC_PEEK_LIMIT: 12,
  DLQ_PEEK_LIMIT: 50,
  MESSAGE_STATE_LIMIT: 100,
  STREAM_BUFFER_LIMIT: 25,
  EVENT_HISTORY_LIMIT: 40,
  RECENT_EVENTS_LIMIT: 20,
  WORKFLOW_ARTIFACT_LIMIT: 50,
  WORKFLOW_ARTIFACT_TIMEOUT_MS: 5000
});

export const API_PATHS = Object.freeze({
  HEALTH: "/v1/healthz",
  VERSION: "/v1/version",
  CONFIG: "/v1/config",
  TOPICS: "/debug/topics",
  METRICS: "/metrics",
  CREATE_TOPIC: "/v1/topics",
  PRODUCE: "/v1/produce",
  ACK: "/v1/ack",
  NACK: "/v1/nack",
  RUN_CANCEL: "/debug/run-cancel",
  RUN_REPLAY: "/debug/run-replay",
  RUN_DEMO: "/debug/run-demo",
  topicLag: (group) => `/debug/topics/lag?group=${encodeURIComponent(group)}`,
  topicPeek: (topic, limit) => `/debug/topics/peek?topic=${encodeURIComponent(topic)}&limit=${limit}`,
  runs: (limit) => `/debug/runs?limit=${limit}`,
  runDetail: (runID) => `/debug/run?run_id=${encodeURIComponent(runID)}`,
  runArtifacts: (runID, limit) => `/debug/run-artifacts?run_id=${encodeURIComponent(runID)}&limit=${limit}`,
  artifactGet: (artifactID) => `/debug/artifact-get?artifact_id=${encodeURIComponent(artifactID)}`,
  consume: ({ topic, group, owner, leaseMs }) => `/v1/consume?topic=${encodeURIComponent(topic)}&group=${encodeURIComponent(group)}&owner=${encodeURIComponent(owner)}&lease_ms=${leaseMs}`,
  messageState: (query) => `/debug/messages/state?${query.toString()}`
});

export const HTTP = Object.freeze({
  METHOD_POST: "POST",
  CONTENT_TYPE_HEADER: "Content-Type",
  CONTENT_TYPE_JSON: "application/json",
  STREAMING_BODY_UNAVAILABLE: "streaming body unavailable",
  STREAM_CANCELLED: "stream cancelled",
  CONSUME_TIMED_OUT: "consume timed out",
  REQUEST_ABORTED: "request aborted",
  NO_MESSAGE_RECEIVED: "no message received",
  CONSUME_CANCELLED: "consume cancelled"
});

export const METRIC_NAME = Object.freeze({
  PRODUCE_REJECTED_TOTAL: "produce_rejected_total",
  DLQ_MESSAGES_TOTAL: "dlq_messages_total",
  CONSUMER_LAG: "consumer_lag",
  INFLIGHT_MESSAGES: "inflight_messages",
  TOPIC_CREATED_TOTAL: "topic_created_total",
  MESSAGE_ACKS_TOTAL: "message_acks_total",
  MESSAGE_NACKS_TOTAL: "message_nacks_total",
  MESSAGE_LEASE_TIMEOUTS_TOTAL: "message_lease_timeouts_total",
  MESSAGE_REDELIVERIES_TOTAL: "message_redeliveries_total"
});

export const EVENT_TYPE = Object.freeze({
  PRODUCE: "PRODUCE",
  LEASE: "LEASE",
  TOPIC: "TOPIC",
  ACK: "ACK",
  NACK: "NACK",
  LEASE_TIMEOUT: "LEASE_TIMEOUT",
  REDELIVERY: "REDELIVERY",
  DLQ: "DLQ",
  RUN: "RUN",
  HEARTBEAT: "HEARTBEAT"
});

export const EVENT_GROUP = Object.freeze({
  BROKER: "broker",
  SYSTEM: "system",
  LOCAL: "local",
  WORKFLOW: "workflow",
  WORKFLOW_VERSION: "v2",
  NODE: "node",
  DLQ: "dlq"
});

export const CONSUMER_STATUS = Object.freeze({
  CONNECTED: "connected",
  BACKLOG: "backlog",
  IDLE: "idle"
});

export const MESSAGE_STATE = Object.freeze({
  ALL: "all",
  QUEUED: "queued",
  IN_FLIGHT: "in_flight",
  ACKED: "acked",
  RETRIED: "retried",
  DEAD_LETTERED: "dead_lettered"
});

export const WORKFLOW_STATUS = Object.freeze({
  SUCCEEDED: "succeeded",
  COMPLETED: "completed",
  FAILED: "failed",
  CANCELED: "canceled",
  WAITING: "waiting",
  RUNNING: "running",
  LOADING: "loading",
  LOADED: "loaded",
  ERROR: "error",
  IDLE: "idle"
});

export const WORKFLOW_REPLAY_MODE = Object.freeze({
  TIME_TRAVEL: "time_travel",
  LIVE: "live"
});

export const TOPIC_PREFIX = Object.freeze({
  DLQ: "dlq."
});

export const APP_COPY = Object.freeze({
  REFRESH_ERROR_PREFIX: "refresh error:",
  FOOTER_LABEL: "DriftQ Dashboard - embedded at :8080/ui"
});

export const HEADER_COPY = Object.freeze({
  BRAND_DRIFT: "Drift",
  BRAND_Q: "Q",
  SUBTITLE: "Dashboard",
  UPDATED: "updated",
  LAST_UPDATE: "last update"
});

export const CONTROLS_COPY = Object.freeze({
  GROUP_LABEL: "Group",
  REFRESH_BUTTON: "Refresh",
  groupPlaceholder: DEFAULTS.GROUP,
  tickLabel: (tick) => `tick #${tick}`
});

export const DASHBOARD_COPY = Object.freeze({
  REFRESH_FAILED: "refresh failed",
  DISCONNECTED_RETRYING: "Disconnected from broker. Retrying...",
  NETWORK_FETCH_FAILED: "Failed to fetch",
  NETWORK_ERROR_TOKEN: "NetworkError",
  HEALTH_FAILED: "health failed",
  VERSION_FAILED: "version failed",
  CONFIG_FAILED: "config failed",
  TOPICS_FAILED: "topics failed",
  LAG_FAILED: "lag failed",
  METRICS_FAILED: "metrics failed",
  RUNS_FAILED: "runs failed",
  DLQ_PEEK_FAILED: "dlq peek failed"
});

export const OVERVIEW_COPY = Object.freeze({
  BYTE_UNITS: Object.freeze(["B", "KB", "MB", "GB", "TB"]),
  HELP_TRIGGER: "?",
  EVENTS_IDLE_NOTE: "Waiting for broker activity. Produce, consume, ack, or nack a message to see live events here.",
  SNAPSHOT_NOTE: "Quick read of where this node is listening, how it stores state, and which runtime limits are active.",
  ADVANCED_CONFIG: "Advanced Broker Config",
  ADVANCED_CONFIG_NOTE: "Lower-frequency broker settings for durability, logging, and artifact storage.",
  PRIMARY_METRICS: Object.freeze({
    PRODUCED: "Messages Produced",
    CONSUMED: "Messages Consumed",
    IN_FLIGHT: "In-Flight",
    DEAD_LETTERS: "Dead Letters"
  }),
  SECONDARY_METRICS: Object.freeze({
    ACTIVE_PRODUCERS: "Active Producers",
    NOT_TRACKED: "Not tracked",
    ACTIVE_PRODUCERS_DESCRIPTION: "Producer-count tracking is not exposed by the broker yet.",
    ACTIVE_PRODUCERS_TOOLTIP: "This card will become numeric once the backend exposes producer tracking.",
    CONSUMER_GROUPS: "Consumer Groups",
    CONSUMER_GROUPS_DESCRIPTION: "Distinct consumer groups visible in the current dashboard snapshot.",
    DEDUPLICATED: "Deduplicated",
    DEDUPLICATED_DESCRIPTION: "Deduplication totals are not exposed by the broker yet.",
    DEDUPLICATED_TOOLTIP: "This card will become numeric once the backend publishes deduplication counters.",
    BACKPRESSURE_REJECTED: "Backpressure Rejected",
    BACKPRESSURE_REJECTED_DESCRIPTION: "Produce requests rejected because broker safety limits were hit."
  }),
  TOPIC_THROUGHPUT: "Topic Throughput",
  LIVE_EVENT_STREAM: "Live Event Stream",
  BROKER_SNAPSHOT: "Broker Snapshot",
  TABLE_HEADERS: Object.freeze({
    TOPIC: "Topic",
    RATE: "Rate",
    LAG: "Lag",
    TREND: "Trend"
  }),
  SNAPSHOT_ROWS: Object.freeze({
    WAL_PATH_LABEL: "WAL Path",
    WAL_PATH_DESCRIPTION: "Filesystem location of the broker write-ahead log file.",
    WAL_PATH_TOOLTIP: "If WAL is enabled, broker writes are appended here so they can be replayed after a restart.",
    WAL_ENGINE_LABEL: "WAL Engine",
    WAL_ENGINE_DESCRIPTION: "Durability backend used by the workflow engine side of DriftQ.",
    WAL_ENGINE_TOOLTIP: "This identifies which write-ahead log implementation the workflow engine is currently using.",
    LOG_MODE_LABEL: "Log Mode",
    LOG_MODE_DESCRIPTION: "Current log verbosity and output format for server logs.",
    LOG_MODE_TOOLTIP: "The first value is the log level and the second is the output format written by the server.",
    ACCESS_LOG_LABEL: "Access Log",
    ACCESS_LOG_ENABLED: "enabled",
    ACCESS_LOG_DISABLED: "disabled",
    ACCESS_LOG_DESCRIPTION: "Whether HTTP requests are written to the request log.",
    ACCESS_LOG_TOOLTIP: "When enabled, incoming API and UI requests are logged for debugging and audit trails.",
    WAL_SYNC_LABEL: "WAL Sync",
    WAL_SYNC_DESCRIPTION: "How often buffered WAL data is forced to durable storage.",
    WAL_SYNC_TOOLTIP: "Shorter intervals improve durability but may reduce throughput because writes are flushed more often.",
    WAL_BUFFER_LABEL: "WAL Buffer",
    WAL_BUFFER_DESCRIPTION: "Amount of WAL data that can be buffered before a flush.",
    WAL_BUFFER_TOOLTIP: "A larger buffer can improve throughput, but increases the amount of unwritten data held in memory.",
    ARTIFACTS_DIR_LABEL: "Artifacts Dir",
    ARTIFACTS_DIR_DESCRIPTION: "Directory where workflow artifacts and generated outputs are stored.",
    ARTIFACTS_DIR_TOOLTIP: "This is where files produced by workflow steps are persisted for later inspection or download."
  }),
  SNAPSHOT_SUMMARY: Object.freeze({
    ENDPOINT_LABEL: "Endpoint",
    ENDPOINT_DESCRIPTION: "Where this DriftQ node is listening for API and dashboard traffic.",
    ENDPOINT_TOOLTIP: "This is the network address clients use to talk to the broker and open the embedded UI.",
    ENDPOINT_VALUE_PREFIX: "Listening on",
    BUILD_PREFIX: "Build",
    BUILD_DETAIL: "Version of the running DriftQ server build.",
    WAL_ENABLED: "Durability WAL enabled",
    WAL_DISABLED: "Durability in-memory only",
    WAL_DETAIL: "Whether broker writes are persisted to the write-ahead log.",
    STORAGE_LABEL: "Storage",
    STORAGE_DESCRIPTION: "How broker and workflow state are stored while the node is running.",
    STORAGE_TOOLTIP: "This summarizes the active storage engine, durability backend, and where artifacts are written.",
    STORAGE_VALUE_PREFIX: "State stored in",
    ENGINE_WAL_PREFIX: "Engine WAL",
    ENGINE_WAL_DETAIL: "Workflow engine write-ahead log backend.",
    ARTIFACTS_PREFIX: "Artifacts",
    ARTIFACTS_DETAIL: "Directory used to store workflow outputs and artifacts.",
    LIMITS_LABEL: "Limits",
    LIMITS_DESCRIPTION: "Active safety caps that bound how much work the broker will accept at once.",
    LIMITS_TOOLTIP: "These caps prevent too many unacked leases or oversized partitions from overwhelming the broker.",
    LIMITS_VALUE_TEMPLATE: (value) => `Allows ${value} unacked messages in flight`,
    PARTITION_MSGS_PREFIX: "Partition cap",
    PARTITION_MSGS_SUFFIX: "msgs",
    PARTITION_MSGS_DETAIL: "Maximum number of messages allowed in a single partition.",
    PARTITION_BYTES_DETAIL: "Maximum total bytes allowed in a single partition.",
    RUNTIME_LABEL: "Runtime",
    RUNTIME_DESCRIPTION: "How the server logs activity and flushes durable writes while it is running.",
    RUNTIME_TOOLTIP: "This combines the current log verbosity, output format, request logging, and WAL sync cadence.",
    RUNTIME_VALUE_TEMPLATE: (level, format) => `Logs at ${level} level in ${format} format`,
    ACCESS_LOG_ENABLED: "HTTP access logging enabled",
    ACCESS_LOG_DISABLED: "HTTP access logging disabled",
    ACCESS_LOG_DETAIL: "Whether incoming HTTP requests are written to the access log.",
    WAL_FLUSH_PREFIX: "WAL flush interval",
    WAL_FLUSH_DETAIL: "How often buffered WAL writes are forced to durable storage."
  })
});

export const TOPICS_COPY = Object.freeze({
  TOPIC_NAME_REQUIRED: "topic name is required",
  PARTITIONS_MINIMUM: "partitions must be >= 1",
  CREATED_TOPIC_PREFIX: "created topic",
  CREATE_TOPIC_FAILED: "failed to create topic",
  LOAD_TOPIC_MESSAGES_FAILED: "failed to load topic messages",
  CREATE_TOPIC: "Create Topic",
  CREATE_TOPIC_DESCRIPTION: "Provision a topic directly from the dashboard.",
  NAME: "Name",
  PARTITIONS: "Partitions",
  NAME_PLACEHOLDER: "orders",
  CREATING: "Creating...",
  CREATE_TOPIC_BUTTON: "Create Topic",
  REFRESH_PEEK: "Refresh Peek",
  INSPECT: "Inspect",
  PRODUCED: "produced",
  CONSUMED: "consumed",
  INFLIGHT: "inflight",
  DLQ: "DLQ",
  TOPIC_INSPECTOR: "Topic Inspector",
  RECENT_MESSAGES_PREFIX: "Recent messages for",
  LOADING: "Loading...",
  REFRESH: "Refresh",
  CLOSE: "Close",
  NO_MESSAGES_YET: "no messages in this topic yet",
  OFFSET: "Offset",
  KEY: "Key",
  ATTEMPTS: "Attempts",
  LAST_ERROR: "Last Error",
  SELECTED: "Selected",
  OFFSET_PREFIX: "offset",
  PARTITION_PREFIX: "partition",
  ATTEMPTS_PREFIX: "attempts"
});

export const PRODUCERS_COPY = Object.freeze({
  TOPIC_REQUIRED: "topic is required",
  MESSAGE_VALUE_REQUIRED: "message value is required",
  PRODUCED_TO_PREFIX: "produced to",
  PRODUCE_FAILED: "failed to produce message",
  PRODUCE_MESSAGE: "Produce Message",
  PRODUCE_DESCRIPTION: "Send a test message without leaving the dashboard.",
  CREATE_TOPIC_FIRST: "Create a topic first in the Topics tab, then come back here to send a message.",
  TOPIC: "Topic",
  KEY: "Key",
  KEY_PLACEHOLDER: "optional-key",
  SEND_MESSAGE: "Send Message",
  SENDING: "Sending...",
  VALUE: "Value",
  VALUE_PLACEHOLDER: "{\"hello\":\"driftq\"}",
  COUNTER_NOTE: "Producer identity is not currently exposed by DriftQ API. This tab uses real broker counters from",
  REJECTIONS_BY_REASON: "Produce Rejections by Reason",
  REASON: "Reason",
  COUNT: "Count",
  NO_REJECTION_METRICS: "no rejection metrics yet"
});

export const CONSUMERS_COPY = Object.freeze({
  STREAM_STOPPED: "stream stopped",
  TEMP_STREAM_STOPPED: "temporary stream stopped",
  PICK_TOPIC: "pick a topic to consume from",
  OWNER_REQUIRED: "owner is required",
  STREAMING_PREFIX: "streaming",
  STREAMING_FOR: "for",
  TEMP_STREAM_ENDED: "temporary stream ended",
  CONSUME_STREAM_FAILED: "failed to consume stream",
  STREAM_BUFFER_CLEARED: "stream buffer cleared",
  ACK_FAILED: "failed to ack message",
  NACK_FAILED: "failed to nack message",
  ACKED_OFFSET_PREFIX: "acked offset",
  NACKED_OFFSET_PREFIX: "nacked offset",
  NO_TOPICS: "no topics",
  ACTIVE_LEASES: "active leases",
  LAG: "lag",
  PARTITIONS: "partitions",
  OWNERS: "owners",
  STALLED: "stalled",
  STREAMING: "Streaming",
  INSPECTING: "Inspecting",
  INSPECT: "Inspect",
  OFFSET: "Offset",
  ATTEMPTS: "Attempts",
  DETAIL_TITLE: "Consumer Group Detail",
  DETAIL_PREFIX: "Deep view for",
  DETAIL_EMPTY: "No consumer groups detected yet.",
  NO_CONSUMER_GROUPS: "No consumer groups to inspect yet.",
  NO_ACTIVE_OWNER: "no active owner",
  LAST_DELIVERY_PREFIX: "last delivery",
  TABLE_HEADERS: Object.freeze({
    TOPIC: "Topic",
    PARTITION: "Partition",
    OWNER: "Owner",
    LAST_DELIVERY: "Last Delivery",
    LEASE_AGE: "Lease Age",
    HEAD: "Head",
    COMMITTED: "Committed",
    LAG: "Lag",
    INFLIGHT: "Inflight",
    STATE: "State"
  }),
  STATE_LABELS: Object.freeze({
    STALLED: "stalled",
    LEASED: "leased",
    WAITING: "waiting",
    CAUGHT_UP: "caught up"
  }),
  NO_PARTITION_DETAIL: "no partition detail available for this consumer group",
  TEMP_STREAM_TITLE: "Temporary Consumer Stream",
  TEMP_STREAM_DESCRIPTION: "Open a live leased stream for one group/topic/owner tuple, then inspect and ack or nack messages as they arrive.",
  LIVE: "live",
  STOPPED: "stopped",
  GROUP: "Group",
  TOPIC: "Topic",
  OWNER: "Owner",
  OWNER_PLACEHOLDER: DEFAULTS.CONSUMER_OWNER,
  LEASE_MS: "Lease Ms",
  STOP_STREAM: "Stop Stream",
  START_STREAM: "Start Stream",
  CLEAR_BUFFER: "Clear Buffer",
  GROUP_PREFIX: "group",
  OWNER_PREFIX: "owner",
  LEASE_PREFIX: "lease",
  LIVE_STREAM: "live stream",
  LAST_SESSION: "last session",
  RECEIVED: "received",
  BUFFERED: "buffered",
  RECEIVED_AT: "Received",
  ERROR: "Error",
  STREAMED_DETAIL_TITLE: "Streamed Message Detail",
  STREAMED_DETAIL_EMPTY: "Select a streamed message to inspect it.",
  NACK_REASON: "Nack Reason",
  NACK_REASON_PLACEHOLDER: DEFAULTS.CONSUMER_NACK_REASON,
  ACK: "Ack",
  ACKING: "Acking...",
  NACK: "Nack",
  NACKING: "Nacking...",
  NO_STREAMED_MESSAGE: "No streamed message selected.",
  STREAM_WAITING: "Stream is open and waiting for messages.",
  STREAM_EMPTY: "No streamed messages yet. Start the temporary stream to watch messages arrive live.",
  ATTACH_GROUP_FIRST: "Create or attach a consumer group first so there is something to inspect."
});

export const MESSAGE_STATE_OPTIONS = Object.freeze([
  { value: MESSAGE_STATE.ALL, label: "All states" },
  { value: MESSAGE_STATE.QUEUED, label: "Queued" },
  { value: MESSAGE_STATE.IN_FLIGHT, label: "In-Flight" },
  { value: MESSAGE_STATE.ACKED, label: "Acked" },
  { value: MESSAGE_STATE.RETRIED, label: "Retried" },
  { value: MESSAGE_STATE.DEAD_LETTERED, label: "Dead-Lettered" }
]);

export const MESSAGES_COPY = Object.freeze({
  LOAD_STATE_FAILED: "failed to load message state",
  BROWSER_TITLE: "Message State Browser",
  BROWSER_DESCRIPTION_PREFIX: "Filter broker-visible messages by topic, owner, and state for group",
  TOPIC: "Topic",
  ALL_TOPICS: "All topics",
  STATUS: "Status",
  OWNER: "Owner",
  OWNER_PLACEHOLDER: "ownerA",
  COUNTS: Object.freeze({
    QUEUED: "queued",
    IN_FLIGHT: "in-flight",
    ACKED: "acked",
    RETRIED: "retried",
    DEAD_LETTERED: "dead-lettered",
    ROWS: "rows"
  }),
  INSPECTING: "Inspecting",
  INSPECT: "Inspect",
  TABLE_HEADERS: Object.freeze({
    TOPIC: "Topic",
    PARTITION: "Partition",
    OFFSET: "Offset",
    STATE: "State",
    OWNER: "Owner",
    ATTEMPTS: "Attempts",
    LAST_DELIVERY: "Last Delivery",
    LEASE_AGE: "Lease Age",
    ERROR: "Error"
  }),
  LOADING_STATE: "loading message state...",
  NO_MATCHING_MESSAGES: "no messages matched the current filters",
  DETAIL_TITLE: "Message Detail",
  DETAIL_EMPTY: "Select a message row to inspect it.",
  LEASE_PREFIX: "lease",
  EXPIRES_PREFIX: "expires",
  NO_MESSAGE_SELECTED: "No message selected."
});

export const DEAD_LETTERS_COPY = Object.freeze({
  PICK_TARGET_TOPIC: "pick a target topic",
  REDRIVE_FAILED: "failed to re-drive DLQ message",
  REDRIVE_PREFIX: "re-driven to",
  DLQ_TOPIC: "DLQ Topic",
  INPUT_PLACEHOLDER: "dlq.my-topic",
  TABLE_HEADERS: Object.freeze({
    ID: "ID",
    DLQ_TOPIC: "DLQ Topic",
    ORIGINAL_TOPIC: "Original Topic",
    REASON: "Reason",
    RETRIES: "Retries",
    FAILED_AT: "Failed At"
  }),
  NO_MESSAGES: "no DLQ messages",
  CLOSE: "Close",
  INSPECT: "Inspect",
  REDRIVE_NOTE: "Re-drive republishes this payload to another topic. It does not remove the original message from the DLQ because the broker does not expose delete semantics here.",
  ORIGINAL_PREFIX: "original",
  PARTITION_PREFIX: "partition",
  OFFSET_PREFIX: "offset",
  ATTEMPTS_PREFIX: "attempts",
  TARGET_TOPIC: "Target Topic",
  REDRIVING: "Re-driving...",
  REDRIVE_BUTTON: "Re-drive Message"
});

export const WORKFLOWS_COPY = Object.freeze({
  ARTIFACT_LIST_TIMED_OUT: "artifact list timed out",
  LOAD_ARTIFACTS_FAILED: "failed to load artifacts",
  CANCEL_FAILED: "failed to cancel run",
  REPLAY_FAILED: "failed to replay run",
  START_DEMO_FAILED: "failed to start demo run",
  ARTIFACT_PREVIEW_FAILED_PREFIX: "artifact preview failed:",
  BINARY_PREVIEW_UNSUPPORTED: "binary artifact preview is not supported inline",
  PREVIEW_FAILED: "failed to preview artifact",
  CANCELED_RUN_PREFIX: "canceled run",
  REPLAYED_RUN_PREFIX: "replayed run",
  FROM: "from",
  STARTED_DEMO_RUN_PREFIX: "started demo run",
  UNKNOWN_TYPE: "unknown type",
  HIDE: "Hide",
  INSPECT: "Inspect",
  WORKFLOW_PREFIX: "workflow",
  STARTED_PREFIX: "started",
  CONTROLS_TITLE: "Workflow Controls",
  CONTROLS_DESCRIPTION: "Inspect runs, replay from a step, cancel active work, and inspect artifacts.",
  STARTING: "Starting...",
  RUN_DEMO: "Run Demo",
  LIVE_REPLAY_HINT: "live replay forces the selected step and downstream steps to run again.",
  TIME_TRAVEL_SELECTED_HINT: "time_travel reuses succeeded outputs. If this step already succeeded and there is nothing downstream to rerun, the replay will be a no-op.",
  TIME_TRAVEL_HINT: "time_travel reuses succeeded outputs when possible.",
  SUMMARY_LABELS: Object.freeze({
    STATUS: "Status",
    WORKFLOW: "Workflow",
    STARTED: "Started",
    ENDED: "Ended",
    DURATION: "Duration",
    TERMINAL_REASON: "Terminal Reason"
  }),
  COUNTS: Object.freeze({
    DONE: "done",
    RUNNING: "running",
    WAITING: "waiting",
    FAILED: "failed",
    CANCELED: "canceled"
  }),
  STEP_TABLE_HEADERS: Object.freeze({
    STEP: "Step",
    STATUS: "Status",
    ATTEMPTS: "Attempts",
    DURATION: "Duration",
    BYTES: "Bytes",
    ERROR: "Error"
  }),
  STEP_DETAIL_TITLE: "Step Detail",
  IO_BYTES: "I/O Bytes",
  NO_STEP_ERROR: "No step error recorded.",
  STEP_ERROR_PREFIX: "error:",
  REPLAY_FROM: "Replay From",
  ENTIRE_RUN: "entire run",
  REPLAY_MODE: "Replay Mode",
  CANCEL_REASON: "Cancel Reason",
  CANCEL_REASON_PLACEHOLDER: DEFAULTS.WORKFLOW_CANCEL_REASON,
  REPLAYING: "Replaying...",
  REPLAY_RUN: "Replay Run",
  CANCELING: "Canceling...",
  CANCEL_RUN: "Cancel Run",
  ARTIFACTS: "Artifacts",
  ITEMS: "items",
  ARTIFACT_HEADERS: Object.freeze({
    ARTIFACT: "Artifact",
    NODE: "Node",
    TYPE: "Type",
    SIZE: "Size",
    CREATED: "Created",
    ACTIONS: "Actions"
  }),
  PREVIEW: "Preview",
  OPEN: "Open",
  NO_ARTIFACTS: "No artifacts recorded for this run yet.",
  ARTIFACT_PREVIEW: "Artifact Preview",
  LOADING_PREVIEW: "loading preview...",
  LOADING: "loading...",
  STEP_ACTION_ATTEMPT_PREFIX: "attempt",
  NO_RUNS: "No workflow runs yet. Start one with",
  STEP_STATUS_EMPTY: COMMON_TEXT.UNKNOWN
});

export const TIME = Object.freeze({
  LOCALE: "en-US"
});
