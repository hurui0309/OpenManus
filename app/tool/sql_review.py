"""SQL Review tool for analyzing and optimizing SQL queries."""
import logging
from typing import Dict, Any, Optional

from pydantic import Field

from app.config import Config
from app.exceptions import ReviewError
from app.llm import LLM
from app.prompt.sql_review import (
    SQL_REVIEW_SYSTEM_PROMPT,
    SQL_REVIEW_USER_PROMPT,
    SQL_REVIEW_ASSISTANT_PROMPT
)
from app.tool.base import BaseTool, ToolResult
from app.tool.db_operations import DatabaseOperations

logger = logging.getLogger(__name__)

class SQLReviewTool(BaseTool):
    """SQL Review工具类，用于分析和优化SQL查询。"""

    name: str = "sql_review"
    description: str = "Analyzes and optimizes SQL queries, providing comprehensive review feedback"
    parameters: Dict = {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SQL query to review"
            }
        },
        "required": ["sql"]
    }

    config: Config = Field(...)
    llm: Optional[LLM] = None
    db_ops: Optional[DatabaseOperations] = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        self.llm = LLM(config_name="default")
        self.db_ops = DatabaseOperations(self.config)

    async def execute(self, **kwargs) -> ToolResult:
        """执行SQL Review。

        Args:
            sql: 待review的SQL语句

        Returns:
            ToolResult包含review结果
        """
        sql = kwargs.get("sql")
        if not sql:
            return ToolResult(error="SQL query is required")

        try:
            # 准备prompt
            messages = [
                {"role": "system", "content": SQL_REVIEW_SYSTEM_PROMPT},
                {"role": "user", "content": SQL_REVIEW_USER_PROMPT.format(sql=sql)},
                {"role": "assistant", "content": SQL_REVIEW_ASSISTANT_PROMPT}
            ]

            # 调用LLM进行review
            response = await self.llm.ask(
                messages=messages,
                temperature=0.2,
                stream=False
            )

            # 解析LLM响应
            review_result = self._parse_review_response(response)

            # 确定问题严重程度
            severity = self._determine_severity(review_result)

            try:
                # 保存review结果到数据库
                review_id = self.db_ops.save_sql_review(
                    sql_text=sql,
                    review_result=review_result,
                    severity=severity,
                    reviewer="AI"
                )
                # 在结果中添加review_id
                review_result["review_id"] = review_id
            except Exception as db_error:
                logger.error(f"Failed to save SQL review to database: {str(db_error)}")
                # 数据库错误不影响review结果的返回
                review_result["db_error"] = str(db_error)

            # 将字典格式化为字符串
            result_str = f"""SQL Review 结果：

## 总体评价
{review_result.get('overall_assessment', '无')}

## 问题列表
{chr(10).join(f"{i+1}. {issue}" for i, issue in enumerate(review_result.get('issues', [])))}

## 改进建议
{review_result.get('recommendations', '无')}

## 优化SQL
```sql
{review_result.get('optimized_sql', sql)}
```

## 补充说明
{review_result.get('additional_notes', '无')}
"""
            return ToolResult(output=result_str)

        except Exception as e:
            error_msg = f"SQL review failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return ToolResult(error=error_msg)

    def _parse_review_response(self, response: str) -> Dict[str, Any]:
        """解析LLM的响应内容，提取结构化的review结果。

        Args:
            response: LLM的响应文本

        Returns:
            Dict包含解析后的review结果

        Raises:
            ReviewError: 当解析响应失败时
        """
        try:
            sections = response.split("##")
            result = {}

            for section in sections:
                if not section.strip():
                    continue

                lines = section.strip().split("\n")
                if not lines:
                    continue

                title = lines[0].strip()
                content = "\n".join(lines[1:]).strip()

                if "总体评价" in title:
                    result["overall_assessment"] = content
                elif "问题列表" in title:
                    result["issues"] = [
                        issue.strip()[2:].strip()
                        for issue in content.split("\n")
                        if issue.strip() and issue.strip()[0].isdigit()
                    ]
                elif "改进建议" in title:
                    result["recommendations"] = content
                elif "优化SQL" in title:
                    sql_lines = content.split("```")
                    if len(sql_lines) >= 3:
                        result["optimized_sql"] = sql_lines[1].strip()
                elif "补充说明" in title:
                    result["additional_notes"] = content

            # 验证必要字段
            required_fields = ["overall_assessment", "issues", "recommendations"]
            missing_fields = [field for field in required_fields if field not in result]
            if missing_fields:
                raise ReviewError(f"Missing required fields in review response: {missing_fields}")

            return result

        except Exception as e:
            raise ReviewError(f"Failed to parse review response: {str(e)}")

    def _determine_severity(self, review_result: Dict[str, Any]) -> str:
        """根据review结果确定问题的严重程度。

        Args:
            review_result: Review结果字典

        Returns:
            严重程度: "critical", "major", "minor", "info"
        """
        try:
            issues = review_result.get("issues", [])
            if not issues:
                return "info"

            # 根据问题描述中的关键词判断严重程度
            critical_keywords = {"安全风险", "数据泄露", "注入", "性能灾难"}
            major_keywords = {"全表扫描", "性能问题", "死锁", "资源消耗"}
            minor_keywords = {"规范", "格式", "命名", "注释"}

            for issue in issues:
                for keyword in critical_keywords:
                    if keyword in issue:
                        return "critical"

            for issue in issues:
                for keyword in major_keywords:
                    if keyword in issue:
                        return "major"

            for issue in issues:
                for keyword in minor_keywords:
                    if keyword in issue:
                        return "minor"

            return "info"

        except Exception as e:
            logger.warning(f"Error determining severity: {str(e)}")
            return "info"  # 默认返回info级别
