NOTE_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "summary": {"type": "string"},
        "context_and_objective": {"type": "string"},
        "main_topics": {
            "type": "array",
            "items": {"type": "string"},
        },
        "participant_contributions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "role": {"type": "string"},
                    "contributions": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["name", "role", "contributions"],
                "additionalProperties": False,
            },
        },
        "decisions": {
            "type": "array",
            "items": {"type": "string"},
        },
        "decision_details": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "decision": {"type": "string"},
                    "status": {"type": "string"},
                    "priority": {"type": "string"},
                },
                "required": ["decision", "status", "priority"],
                "additionalProperties": False,
            },
        },
        "action_items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "task": {"type": "string"},
                    "owner": {"type": "string"},
                    "due_date": {"type": "string"},
                    "status": {"type": "string"},
                    "priority": {"type": "string"},
                },
                "required": ["task", "owner", "due_date", "status", "priority"],
                "additionalProperties": False,
            },
        },
        "risks": {
            "type": "array",
            "items": {"type": "string"},
        },
        "open_questions": {
            "type": "array",
            "items": {"type": "string"},
        },
        "open_items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "item": {"type": "string"},
                    "status": {"type": "string"},
                },
                "required": ["item", "status"],
                "additionalProperties": False,
            },
        },
        "tags": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "required": [
        "title",
        "summary",
        "context_and_objective",
        "main_topics",
        "participant_contributions",
        "decisions",
        "decision_details",
        "action_items",
        "risks",
        "open_questions",
        "open_items",
        "tags",
    ],
    "additionalProperties": False,
}
