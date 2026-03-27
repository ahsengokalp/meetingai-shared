from __future__ import annotations

from typing import TypedDict


class ParticipantContributionDTO(TypedDict):
    name: str
    role: str
    contributions: list[str]


class DecisionDetailDTO(TypedDict):
    decision: str
    status: str
    priority: str


class ActionItemDTO(TypedDict):
    task: str
    owner: str
    due_date: str
    status: str
    priority: str


class OpenItemDTO(TypedDict):
    item: str
    status: str


class MeetingNoteDTO(TypedDict):
    title: str
    summary: str
    context_and_objective: str
    main_topics: list[str]
    participant_contributions: list[ParticipantContributionDTO]
    decisions: list[str]
    decision_details: list[DecisionDetailDTO]
    action_items: list[ActionItemDTO]
    risks: list[str]
    open_questions: list[str]
    open_items: list[OpenItemDTO]
    tags: list[str]
