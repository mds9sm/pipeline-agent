# Chat Interface

The chat interface is the primary way to interact with DAPOS. Send natural language commands and the agent routes them to the appropriate action.

---

## How It Works

```
User Input → route_command() → Action Classification → Handler → Response
```

1. User types a message in the chat box or via CLI
2. `route_command()` classifies intent (keyword match → LLM classification fallback)
3. The appropriate handler executes (list pipelines, trigger run, diagnose, etc.)
4. Response returned with structured data + natural language explanation

---

## Supported Actions

### Pipeline Operations
| What to Say | What Happens |
|-------------|--------------|
| "list pipelines" / "show active pipelines" | Returns pipeline list with status |
| "trigger stripe pipeline" | Triggers manual run |
| "pause orders pipeline" | Pauses scheduled execution |
| "resume orders pipeline" | Resumes scheduling |
| "show runs for stripe" | Lists recent run history |

### Discovery & Creation
| What to Say | What Happens |
|-------------|--------------|
| "I have a MySQL database" | Starts guided pipeline creation flow |
| "connect to Stripe" | Begins source connection flow |
| "set up a pipeline from Oracle to Snowflake" | Multi-turn creation conversation |

### Diagnostics
| What to Say | What Happens |
|-------------|--------------|
| "why is orders failing" | Diagnoses pipeline with root-cause analysis |
| "what breaks if stripe goes down" | Analyzes downstream impact |
| "are there any anomalies" | Checks platform-wide health |
| "diagnose stripe charges" | Direct diagnosis request |

### Topology
| What to Say | What Happens |
|-------------|--------------|
| "I need to merge data from 3 MySQL tables into one" | Designs fan-in topology |
| "design an SCD Type 2 pipeline" | Proposes historical tracking architecture |
| "I have orders and customers that need daily aggregation" | Designs cascading pipeline topology |

### Information
| What to Say | What Happens |
|-------------|--------------|
| "what can you do" | Lists capabilities |
| "show connectors" | Lists available connectors |
| "what quality gates are available" | Explains 7-check gate |

---

## Multi-Turn Conversations

Complex operations like pipeline creation use stateful conversations:

### Pipeline Creation Flow

**Turn 1**: Describe the source
```
User: "I want to load data from our production MySQL"
Agent: "I'll test the connection. What are the credentials?"
```

**Turn 2**: Connection details
```
User: "Host: db.example.com, port 3306, user: reader, database: ecommerce"
Agent: "Connected! Found 12 tables. Which would you like to replicate?"
```

**Turn 3**: Select table
```
User: "The orders table"
Agent: "Profiled orders: 50,000 rows, PK: order_id, watermark candidate: updated_at.
        I recommend: incremental refresh, merge on order_id, schedule every hour."
```

**Turn 4**: Confirm
```
User: "Looks good, create it"
Agent: "Pipeline 'prod-mysql-orders' created and first run triggered."
```

Session state is maintained in `sessionStorage` — refresh the page to start a new conversation.

---

## CLI Chat

```bash
# Single command
python -m cli chat why is my orders pipeline failing

# Multiple words treated as one message
python -m cli chat list active pipelines

# JSON output for scripting
python -m cli chat what breaks if stripe goes down --json
```

---

## API Endpoint

```
POST /api/command
{
  "text": "why is my orders pipeline failing",
  "session_id": "optional-session-id"
}
```

**Response**:
```json
{
  "response": "Pipeline demo-ecommerce-orders is halted due to...",
  "action": "diagnose_pipeline",
  "data": { ... }
}
```

---

## Interaction Audit

Every chat interaction is logged:
- User input, routed action, parameters
- Agent response, result data
- Token usage (input/output), latency
- Model used, any errors

**View**: `GET /api/interactions` or `GET /api/interactions/export`
