"""SQL任务专用agent."""

from typing import Optional
from pydantic import Field

from app.agent.toolcall import ToolCallAgent
from app.config import config
from app.prompt.sql_agent import SQL_SYSTEM_PROMPT, SQL_NEXT_STEP_PROMPT
from app.tool import (
    DataGeneratorTool,
    SQLReviewTool,
    Terminate,
    ToolCollection
)
from app.schema import ToolChoice


class SQLAgent(ToolCallAgent):
    """专门用于SQL相关任务的agent."""

    name: str = "SQLAgent"
    description: str = "An agent specialized in SQL review and data generation tasks"

    system_prompt: str = SQL_SYSTEM_PROMPT
    next_step_prompt: str = SQL_NEXT_STEP_PROMPT

    max_observe: int = 10000
    max_steps: int = 10

    # 使用REQUIRED模式强制调用工具
    tool_choices: ToolChoice = ToolChoice.REQUIRED

    # 只包含SQL相关工具
    available_tools: ToolCollection = Field(
        default_factory=lambda: ToolCollection(
            SQLReviewTool(config=config),
            DataGeneratorTool(config=config),
            Terminate()
        )
    )

    special_tool_names: list[str] = Field(default_factory=lambda: [Terminate().name])

    async def think(self) -> bool:
        """SQL任务特化的思考逻辑."""
        # 在REQUIRED模式下，总是需要调用工具
        result = await super().think()

        # 如果没有选择工具，强制选择terminate
        if not self.tool_calls:
            # 这种情况下应该由LLM自动选择terminate工具
            # 如果仍然没有工具调用，think会返回True让act处理
            pass

        return result
