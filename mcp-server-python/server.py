import json
import sqlite3
from datetime import datetime, timezone


# ============================================================
# MCP SERVER - KAFKA OFFSET BRIDGE
# Native Python libraries only
# In-memory SQLite simulating PostgreSQL
# ============================================================

SERVER_NAME = "payment-event-mcp-server"
SERVER_VERSION = "1.0.0"

DATABASE = sqlite3.connect(":memory:")
DATABASE.row_factory = sqlite3.Row


# ============================================================
# DATABASE INITIALIZATION
# ============================================================

def initialize_database():
    cursor = DATABASE.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_store (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            aggregate_id TEXT NOT NULL,
            aggregate_type TEXT NOT NULL,
            aggregate_version INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            metadata TEXT,
            occurred_at TEXT NOT NULL,
            recorded_at TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kafka_offsets (
            group_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            committed_offset INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (group_id, topic, partition_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS replay_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            original_offset INTEGER NOT NULL,
            replayed_at TEXT NOT NULL,
            status TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kafka_events (
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            event_offset INTEGER NOT NULL,
            event_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (topic, partition_id, event_offset)
        )
    """)

    DATABASE.commit()


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def current_timestamp():
    return datetime.now(timezone.utc).isoformat()


def json_response(data):
    return json.dumps(data, ensure_ascii=False, indent=2)


def initialize_offset(group_id, topic, partition_id, offset=0):
    DATABASE.execute("""
        INSERT OR IGNORE INTO kafka_offsets (
            group_id,
            topic,
            partition_id,
            committed_offset,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?)
    """, (
        group_id,
        topic,
        partition_id,
        offset,
        current_timestamp()
    ))

    DATABASE.commit()


# ============================================================
# EVENT STORE
# ============================================================

def save_event(event):
    required_fields = [
        "event_id",
        "aggregate_id",
        "aggregate_type",
        "aggregate_version",
        "event_type",
        "payload"
    ]

    for field in required_fields:
        if field not in event:
            return {
                "success": False,
                "error": "Missing required field: " + field
            }

    allowed_types = [
        "TED",
        "BOLETO",
        "DEBITO_AUTOMATICO"
    ]

    if event["event_type"] not in allowed_types:
        return {
            "success": False,
            "error": "Unsupported payment event type"
        }

    if not isinstance(event["aggregate_version"], int):
        return {
            "success": False,
            "error": "aggregate_version must be an integer"
        }

    if event["aggregate_version"] <= 0:
        return {
            "success": False,
            "error": "aggregate_version must be positive"
        }

    if not isinstance(event["payload"], dict):
        return {
            "success": False,
            "error": "payload must be a JSON object"
        }

    now = current_timestamp()

    try:
        DATABASE.execute("""
            INSERT INTO event_store (
                event_id,
                aggregate_id,
                aggregate_type,
                aggregate_version,
                event_type,
                payload,
                metadata,
                occurred_at,
                recorded_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event["event_id"],
            event["aggregate_id"],
            event["aggregate_type"],
            event["aggregate_version"],
            event["event_type"],
            json.dumps(event["payload"]),
            json.dumps(event.get("metadata", {})),
            event.get("occurred_at", now),
            now
        ))

        DATABASE.commit()

        return {
            "success": True,
            "event_id": event["event_id"],
            "message": "Event saved successfully"
        }

    except sqlite3.IntegrityError as error:
        return {
            "success": False,
            "error": str(error)
        }


# ============================================================
# KAFKA SIMULATOR
# ============================================================

def publish_event(topic, partition_id, event):
    if not isinstance(event, dict):
        return {
            "success": False,
            "error": "Event must be a JSON object"
        }

    event_id = event.get("event_id")
    event_type = event.get("event_type")

    if not event_id or not event_type:
        return {
            "success": False,
            "error": "event_id and event_type are required"
        }

    cursor = DATABASE.execute("""
        SELECT COALESCE(MAX(event_offset), -1) + 1
        FROM kafka_events
        WHERE topic = ? AND partition_id = ?
    """, (topic, partition_id))

    next_offset = cursor.fetchone()[0]

    DATABASE.execute("""
        INSERT INTO kafka_events (
            topic,
            partition_id,
            event_offset,
            event_id,
            event_type,
            payload,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        topic,
        partition_id,
        next_offset,
        event_id,
        event_type,
        json.dumps(event),
        current_timestamp()
    ))

    DATABASE.commit()

    return {
        "success": True,
        "topic": topic,
        "partition": partition_id,
        "offset": next_offset,
        "event_id": event_id
    }


# ============================================================
# CONSUMER LAG DIAGNOSTICS
# ============================================================

def diagnose_consumer_lag(group_id, topic, partition_id=0):

    cursor = DATABASE.execute("""
        SELECT COALESCE(MAX(event_offset), -1) + 1
        FROM kafka_events
        WHERE topic = ? AND partition_id = ?
    """, (topic, partition_id))

    latest_offset = cursor.fetchone()[0]

    cursor = DATABASE.execute("""
        SELECT committed_offset
        FROM kafka_offsets
        WHERE group_id = ?
          AND topic = ?
          AND partition_id = ?
    """, (group_id, topic, partition_id))

    row = cursor.fetchone()

    if row is None:
        committed_offset = 0
        initialize_offset(group_id, topic, partition_id)
    else:
        committed_offset = row["committed_offset"]

    lag = max(0, latest_offset - committed_offset)

    if lag == 0:
        status = "HEALTHY"
    elif lag <= 5:
        status = "WARNING"
    else:
        status = "CRITICAL"

    return {
        "group_id": group_id,
        "topic": topic,
        "partition": partition_id,
        "latest_offset": latest_offset,
        "committed_offset": committed_offset,
        "consumer_lag": lag,
        "status": status,
        "diagnosed_at": current_timestamp()
    }


# ============================================================
# OFFSET MANAGEMENT
# ============================================================

def commit_offset(group_id, topic, partition_id, offset):

    if not isinstance(offset, int) or offset < 0:
        return {
            "success": False,
            "error": "Offset must be a non-negative integer"
        }

    cursor = DATABASE.execute("""
        SELECT COALESCE(MAX(event_offset), -1) + 1
        FROM kafka_events
        WHERE topic = ? AND partition_id = ?
    """, (topic, partition_id))

    latest_offset = cursor.fetchone()[0]

    if offset > latest_offset:
        return {
            "success": False,
            "error": "Cannot commit beyond the latest available offset"
        }

    initialize_offset(group_id, topic, partition_id)

    DATABASE.execute("""
        UPDATE kafka_offsets
        SET committed_offset = ?,
            updated_at = ?
        WHERE group_id = ?
          AND topic = ?
          AND partition_id = ?
    """, (
        offset,
        current_timestamp(),
        group_id,
        topic,
        partition_id
    ))

    DATABASE.commit()

    return {
        "success": True,
        "group_id": group_id,
        "topic": topic,
        "partition": partition_id,
        "committed_offset": offset
    }


# ============================================================
# EVENT REPLAY
# ============================================================

def replay_events(
    group_id,
    topic,
    partition_id=0,
    start_offset=0,
    end_offset=None
):

    if not isinstance(start_offset, int) or start_offset < 0:
        return {
            "success": False,
            "error": "start_offset must be a non-negative integer"
        }

    cursor = DATABASE.execute("""
        SELECT COALESCE(MAX(event_offset), -1) + 1
        FROM kafka_events
        WHERE topic = ? AND partition_id = ?
    """, (topic, partition_id))

    latest_offset = cursor.fetchone()[0]

    if end_offset is None:
        end_offset = latest_offset

    if not isinstance(end_offset, int):
        return {
            "success": False,
            "error": "end_offset must be an integer"
        }

    if end_offset < start_offset or end_offset > latest_offset:
        return {
            "success": False,
            "error": "Invalid replay offset range"
        }

    cursor = DATABASE.execute("""
        SELECT event_offset, event_id, event_type, payload
        FROM kafka_events
        WHERE topic = ?
          AND partition_id = ?
          AND event_offset >= ?
          AND event_offset < ?
        ORDER BY event_offset ASC
    """, (
        topic,
        partition_id,
        start_offset,
        end_offset
    ))

    records = cursor.fetchall()

    replayed = []

    for record in records:

        try:
            event = json.loads(record["payload"])

            if not validate_payment_event(event):
                status = "REJECTED"
            else:
                status = "REPLAYED"

        except (ValueError, TypeError):
            status = "REJECTED"

        DATABASE.execute("""
            INSERT INTO replay_log (
                group_id,
                event_id,
                topic,
                partition_id,
                original_offset,
                replayed_at,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            group_id,
            record["event_id"],
            topic,
            partition_id,
            record["event_offset"],
            current_timestamp(),
            status
        ))

        replayed.append({
            "event_id": record["event_id"],
            "event_type": record["event_type"],
            "offset": record["event_offset"],
            "status": status
        })

    DATABASE.commit()

    return {
        "success": True,
        "group_id": group_id,
        "topic": topic,
        "partition": partition_id,
        "replay_range": {
            "start_offset": start_offset,
            "end_offset": end_offset
        },
        "total_events": len(replayed),
        "results": replayed
    }


# ============================================================
# PAYMENT EVENT CONTRACT VALIDATION
# ============================================================

def validate_payment_event(event):

    if not isinstance(event, dict):
        return False

    required = [
        "event_id",
        "event_type",
        "transactionId",
        "amount",
        "currency",
        "payerDocument",
        "payerAccount"
    ]

    for field in required:
        if field not in event or event[field] is None:
            return False

    if event["event_type"] not in [
        "TED",
        "BOLETO",
        "DEBITO_AUTOMATICO"
    ]:
        return False

    try:
        amount = float(event["amount"])
        if amount <= 0:
            return False
    except (ValueError, TypeError):
        return False

    if event["currency"] != "BRL":
        return False

    if event["event_type"] == "TED":
        return bool(
            event.get("receiverDocument")
            and event.get("receiverAccount")
        )

    if event["event_type"] == "BOLETO":
        barcode = str(event.get("barcode", ""))

        return (
            len(barcode) in (44, 45, 46, 47, 48)
            and barcode.isdigit()
            and bool(event.get("dueDate"))
        )

    if event["event_type"] == "DEBITO_AUTOMATICO":
        return bool(
            event.get("agreementId")
            and event.get("scheduledDate")
        )

    return False


# ============================================================
# REPLAY HISTORY
# ============================================================

def get_replay_history(group_id=None):

    if group_id:
        cursor = DATABASE.execute("""
            SELECT *
            FROM replay_log
            WHERE group_id = ?
            ORDER BY id DESC
        """, (group_id,))
    else:
        cursor = DATABASE.execute("""
            SELECT *
            FROM replay_log
            ORDER BY id DESC
        """)

    return [
        dict(row)
        for row in cursor.fetchall()
    ]


# ============================================================
# MCP TOOLS
# ============================================================

MCP_TOOLS = [
    {
        "name": "diagnose_consumer_lag",
        "description": "Diagnoses Kafka consumer lag and committed offsets.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "group_id": {"type": "string"},
                "topic": {"type": "string"},
                "partition_id": {"type": "integer"}
            },
            "required": ["group_id", "topic"]
        }
    },
    {
        "name": "replay_events",
        "description": "Replays payment events from a selected Kafka offset range.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "group_id": {"type": "string"},
                "topic": {"type": "string"},
                "partition_id": {"type": "integer"},
                "start_offset": {"type": "integer"},
                "end_offset": {"type": "integer"}
            },
            "required": ["group_id", "topic"]
        }
    },
    {
        "name": "commit_offset",
        "description": "Updates the committed offset for a consumer group.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "group_id": {"type": "string"},
                "topic": {"type": "string"},
                "partition_id": {"type": "integer"},
                "offset": {"type": "integer"}
            },
            "required": [
                "group_id",
                "topic",
                "offset"
            ]
        }
    },
    {
        "name": "get_replay_history",
        "description": "Returns the event replay history.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "group_id": {"type": "string"}
            }
        }
    }
]


# ============================================================
# MCP JSON-RPC SERVER
# ============================================================

def handle_request(request):

    if not isinstance(request, dict):
        return {
            "jsonrpc": "2.0",
            "id": None,
            "error": {
                "code": -32600,
                "message": "Invalid Request"
            }
        }

    request_id = request.get("id")
    method = request.get("method")
    params = request.get("params", {})

    if method == "initialize":

        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": SERVER_NAME,
                    "version": SERVER_VERSION
                }
            }
        }

    if method == "ping":

        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {}
        }

    if method == "notifications/initialized":
        return None

    if method == "tools/list":

        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": MCP_TOOLS
            }
        }

    if method == "tools/call":

        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        try:

            if tool_name == "diagnose_consumer_lag":

                result = diagnose_consumer_lag(
                    arguments["group_id"],
                    arguments["topic"],
                    arguments.get("partition_id", 0)
                )

            elif tool_name == "replay_events":

                result = replay_events(
                    arguments["group_id"],
                    arguments["topic"],
                    arguments.get("partition_id", 0),
                    arguments.get("start_offset", 0),
                    arguments.get("end_offset")
                )

            elif tool_name == "commit_offset":

                result = commit_offset(
                    arguments["group_id"],
                    arguments["topic"],
                    arguments.get("partition_id", 0),
                    arguments["offset"]
                )

            elif tool_name == "get_replay_history":

                result = get_replay_history(
                    arguments.get("group_id")
                )

            else:

                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": "Unknown tool: " + str(tool_name)
                    }
                }

            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": json_response(result)
                        }
                    ],
                    "isError": not result.get("success", True)
                    if isinstance(result, dict)
                    else False
                }
            }

        except (KeyError, TypeError, ValueError) as error:

            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32602,
                    "message": str(error)
                }
            }

    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {
            "code": -32601,
            "message": "Method not found"
        }
    }


# ============================================================
# TEST DATA
# ============================================================

def load_demo_data():

    initialize_offset(
        "cg-pagamentos-real",
        "payment-events",
        0
    )

    initialize_offset(
        "cg-datamesh-regulatorio",
        "payment-events",
        0
    )

    events = [

        {
            "event_id": "EVT-TED-001",
            "event_type": "TED",
            "transactionId": "TX-TED-001",
            "amount": 1500.00,
            "currency": "BRL",
            "payerDocument": "12345678901",
            "payerAccount": "0001-12345",
            "receiverDocument": "98765432100",
            "receiverAccount": "0002-98765"
        },

        {
            "event_id": "EVT-BOLETO-001",
            "event_type": "BOLETO",
            "transactionId": "TX-BOLETO-001",
            "amount": 350.90,
            "currency": "BRL",
            "payerDocument": "12345678901",
            "payerAccount": "0001-12345",
            "barcode": "12345678901234567890123456789012345678901234",
            "dueDate": "2026-12-10"
        },

        {
            "event_id": "EVT-DEBITO-001",
            "event_type": "DEBITO_AUTOMATICO",
            "transactionId": "TX-DEBITO-001",
            "amount": 89.90,
            "currency": "BRL",
            "payerDocument": "12345678901",
            "payerAccount": "0001-12345",
            "agreementId": "AGREEMENT-001",
            "scheduledDate": "2026-12-15"
        },

        {
            "event_id": "EVT-INVALID-001",
            "event_type": "TED",
            "transactionId": "TX-INVALID-001",
            "amount": -100.00,
            "currency": "BRL",
            "payerDocument": "12345678901",
            "payerAccount": "0001-12345"
        }
    ]

    for event in events:
        publish_event(
            "payment-events",
            0,
            event
        )


# ============================================================
# INTERACTIVE LOCAL SERVER
# ============================================================

def run_server():

    initialize_database()
    load_demo_data()

    print("=" * 60)
    print("MCP SERVER - KAFKA OFFSET BRIDGE")
    print("=" * 60)
    print("Server:", SERVER_NAME)
    print("Version:", SERVER_VERSION)
    print("Consumer Groups:")
    print("- cg-pagamentos-real")
    print("- cg-datamesh-regulatorio")
    print("Transport: JSON-RPC over standard input/output")
    print("Database: SQLite in-memory")
    print("=" * 60)

    print("\nMCP server ready. Send JSON-RPC requests.")
    print("Type exit to stop.\n")

    while True:

        try:
            line = input()

            if line.strip().lower() == "exit":
                break

            if not line.strip():
                continue

            request = json.loads(line)

            response = handle_request(request)

            if response is not None:
                print(json_response(response), flush=True)

        except json.JSONDecodeError:

            print(json_response({
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32700,
                    "message": "Parse error"
                }
            }), flush=True)

        except EOFError:
            break

        except Exception as error:

            print(json_response({
                "jsonrpc": "2.0",
                "id": None,
                "error": {
                    "code": -32603,
                    "message": str(error)
                }
            }), flush=True)

    DATABASE.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_server()
