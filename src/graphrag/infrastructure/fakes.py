"""Deterministic model and business adapters for CI and local demonstrations."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import AsyncIterator, Sequence
from datetime import timedelta
from decimal import Decimal
from typing import TypeVar

from pydantic import BaseModel

from graphrag.domain.errors import AuthorizationError, NotFoundError, ValidationError
from graphrag.domain.models import (
    ActionDraft,
    ChatCompletion,
    ChatDelta,
    EmbeddingResult,
    Entity,
    GraphExtraction,
    IdentityContext,
    LogisticsInfo,
    ModelUsage,
    OrderInfo,
    RefundQuote,
    RerankItem,
    RiskLevel,
    utc_now,
)
from graphrag.domain.ports import KnowledgeRepositoryPort

StructuredT = TypeVar("StructuredT", bound=BaseModel)


class FakeModelProvider:
    def __init__(self, *, dimension: int = 1024, version: str = "fake-v1") -> None:
        self.dimension = dimension
        self.version = version
        self.fail_mode: str | None = None

    def _maybe_fail(self) -> None:
        if self.fail_mode == "timeout":
            raise TimeoutError("injected fake timeout")
        if self.fail_mode == "error":
            raise RuntimeError("injected fake error")

    async def complete(
        self, messages: Sequence[dict[str, str]], *, model: str, timeout: float
    ) -> ChatCompletion:
        self._maybe_fail()
        evidence = next(
            (item["content"] for item in reversed(messages) if item["role"] == "system"), ""
        )
        user = next((item["content"] for item in reversed(messages) if item["role"] == "user"), "")
        if "[证据" in evidence:
            answer = f"根据已授权知识：{evidence.splitlines()[-1][:160]}"
        else:
            answer = f"已处理：{user[:160]}"
        return ChatCompletion(
            content=answer,
            model=model,
            usage=ModelUsage(input_tokens=len(evidence) // 4, output_tokens=len(answer) // 4),
            trace_metadata={"provider": "fake"},
        )

    async def stream(
        self, messages: Sequence[dict[str, str]], *, model: str, timeout: float
    ) -> AsyncIterator[ChatDelta]:
        completion = await self.complete(messages, model=model, timeout=timeout)
        pieces = [
            completion.content[index : index + 12]
            for index in range(0, len(completion.content), 12)
        ]
        for sequence, piece in enumerate(pieces, start=1):
            await asyncio.sleep(0)
            yield ChatDelta(sequence=sequence, content=piece, done=False)
        yield ChatDelta(sequence=len(pieces) + 1, content="", done=True)

    async def structured(
        self,
        messages: Sequence[dict[str, str]],
        *,
        model: str,
        response_model: type[StructuredT],
        timeout: float,
    ) -> StructuredT:
        self._maybe_fail()
        raw = next((item["content"] for item in reversed(messages) if item["role"] == "user"), "{}")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {}
        return response_model.model_validate(payload)

    async def embed(
        self, texts: Sequence[str], *, input_type: str, timeout: float
    ) -> EmbeddingResult:
        self._maybe_fail()
        if input_type not in {"document", "query"}:
            raise ValidationError("Embedding input_type 必须为 document 或 query")
        vectors: list[tuple[float, ...]] = []
        for text in texts:
            if not text.strip():
                raise ValidationError("Embedding 文本不能为空")
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            vector = tuple(
                (digest[index % len(digest)] / 127.5) - 1.0 for index in range(self.dimension)
            )
            vectors.append(vector)
        return EmbeddingResult(
            vectors=tuple(vectors),
            model="fake-embedding",
            dimension=self.dimension,
            version=self.version,
        )

    async def extract(self, text: str, *, timeout: float) -> GraphExtraction:
        self._maybe_fail()
        candidates = sorted(
            set(re.findall(r"[A-Za-z][A-Za-z0-9_-]{2,}|[\u4e00-\u9fff]{2,8}", text))
        )[:8]
        entities = tuple(
            Entity(
                name=item,
                normalized_name=item.casefold(),
                entity_type="Other",
                confidence=0.9,
            )
            for item in candidates
        )
        return GraphExtraction(entities=entities, extractor_version=self.version)

    async def query_entities(self, query: str, *, timeout: float) -> list[str]:
        extraction = await self.extract(query, timeout=timeout)
        return [item.normalized_name for item in extraction.entities]

    async def rerank(
        self, query: str, candidates: Sequence[tuple[str, str]], *, timeout: float
    ) -> list[RerankItem]:
        self._maybe_fail()
        terms = set(query.casefold())
        ranked = [
            RerankItem(
                candidate_id=candidate_id,
                score=len(terms.intersection(content.casefold())) / max(len(terms), 1),
            )
            for candidate_id, content in candidates
        ]
        return sorted(ranked, key=lambda item: (-item.score, item.candidate_id))


class FakeBusinessServices:
    def __init__(self, repository: KnowledgeRepositoryPort) -> None:
        self.repository = repository
        self.orders = {
            "DEMO-1001": OrderInfo(
                order_id="DEMO-1001",
                owner_user_id="demo-user",
                status="已发货",
                items=("星河保温杯",),
                masked_address="上海市***路***号",
            )
        }

    def _get_order(self, identity: IdentityContext, order_id: str) -> OrderInfo:
        order = self.orders.get(order_id)
        if order is None:
            raise NotFoundError("订单不存在")
        if order.owner_user_id != identity.user_id and not identity.has_role("admin"):
            raise AuthorizationError("订单不属于当前用户")
        return order

    async def query(self, identity: IdentityContext, order_id: str) -> OrderInfo:
        return self._get_order(identity, order_id)

    async def create_address_draft(
        self,
        identity: IdentityContext,
        order_id: str,
        masked_address: str,
        idempotency_key: str,
    ) -> ActionDraft:
        order = self._get_order(identity, order_id)
        if order.status != "待发货":
            raise ValidationError("当前订单状态不允许修改地址")
        draft = ActionDraft(
            tenant_id=identity.tenant_id,
            user_id=identity.user_id,
            action_type="update_address",
            idempotency_key=idempotency_key,
            payload={"order_id": order_id, "masked_address": masked_address},
            risk_level=RiskLevel.LOW_WRITE,
            expires_at=utc_now() + timedelta(minutes=30),
        )
        return await self.repository.save_draft(draft)

    async def create_urge_draft(
        self, identity: IdentityContext, order_id: str, idempotency_key: str
    ) -> ActionDraft:
        self._get_order(identity, order_id)
        draft = ActionDraft(
            tenant_id=identity.tenant_id,
            user_id=identity.user_id,
            action_type="urge_delivery",
            idempotency_key=idempotency_key,
            payload={"order_id": order_id},
            risk_level=RiskLevel.LOW_WRITE,
            expires_at=utc_now() + timedelta(minutes=30),
        )
        return await self.repository.save_draft(draft)

    async def calculate(self, identity: IdentityContext, order_id: str, amount: str) -> RefundQuote:
        self._get_order(identity, order_id)
        parsed = Decimal(amount)
        if parsed <= 0 or parsed > Decimal("9999"):
            raise ValidationError("退款金额超出 Fake 阶段允许范围")
        return RefundQuote(order_id=order_id, amount=parsed)

    async def create_draft(
        self, identity: IdentityContext, quote: RefundQuote, idempotency_key: str
    ) -> ActionDraft:
        draft = ActionDraft(
            tenant_id=identity.tenant_id,
            user_id=identity.user_id,
            action_type="refund",
            idempotency_key=idempotency_key,
            payload=quote.model_dump(mode="json"),
            risk_level=RiskLevel.CRITICAL,
            status="pending_approval",
            expires_at=utc_now() + timedelta(minutes=30),
        )
        return await self.repository.save_draft(draft)

    async def query_logistics(self, identity: IdentityContext, order_id: str) -> LogisticsInfo:
        order = self._get_order(identity, order_id)
        return LogisticsInfo(
            order_id=order.order_id,
            status="运输中",
            events=("包裹已从虚构分拨中心发出", "预计明日送达"),
        )


class FakeApprovalService:
    async def submit_fake(self, draft: ActionDraft) -> str:
        return f"fake-approval:{draft.draft_id}"


class FakeOCR:
    async def recognize(self, image: bytes, *, page_number: int, timeout: float) -> str:
        if not image:
            raise ValidationError("OCR 图片不能为空")
        return f"[Fake OCR 第{page_number}页] 虚构图片文字"
