"""自然语言解析工具，用于从混合输入中提取SQL语句和用户要求。"""

import logging
import re
from typing import Dict, Optional, Tuple

from pydantic import BaseModel

from app.config import Config
from app.llm import LLM

logger = logging.getLogger(__name__)


class ParsedRequest(BaseModel):
    """解析后的请求模型。"""

    sql: str
    requirements: str = ""
    confidence: float = 0.0  # 解析置信度 0-1


class TextParser:
    """文本解析器，用于从自然语言中提取SQL和用户要求。"""

    def __init__(self, config: Optional[Config] = None):
        """初始化文本解析器。"""
        self.config = config or Config()
        self.llm = LLM(config_name="default")

    async def parse_mixed_input(self, text: str) -> ParsedRequest:
        """解析混合输入文本，提取SQL和用户要求。

        Args:
            text: 包含SQL和用户要求的混合文本

        Returns:
            ParsedRequest: 包含解析结果的对象
        """
        try:
            # 首先尝试规则解析
            rule_result = self._rule_based_parse(text)
            if rule_result.confidence > 0.8:
                logger.info("使用规则解析成功提取SQL和要求")
                return rule_result

            # 规则解析失败或置信度低，使用AI解析
            logger.info("规则解析置信度不足，使用AI解析")
            ai_result = await self._ai_based_parse(text)
            return ai_result

        except Exception as e:
            logger.error(f"解析文本时发生错误: {str(e)}")
            # 降级处理：将整个文本视为SQL
            return ParsedRequest(sql=text.strip(), requirements="", confidence=0.1)

    def _rule_based_parse(self, text: str) -> ParsedRequest:
        """基于规则的解析方法。

        Args:
            text: 输入文本

        Returns:
            ParsedRequest: 解析结果
        """
        text = text.strip()
        confidence = 0.0
        sql = ""
        requirements = ""

        # 模式1: 明确的SQL代码块 + 要求
        sql_block_pattern = r"```(?:sql)?\s*(.*?)\s*```"
        sql_blocks = re.findall(sql_block_pattern, text, re.DOTALL | re.IGNORECASE)

        if sql_blocks:
            sql = sql_blocks[0].strip()
            # 移除SQL代码块，剩余部分作为要求
            requirements = re.sub(
                sql_block_pattern, "", text, flags=re.DOTALL | re.IGNORECASE
            ).strip()
            confidence = 0.9
            logger.debug("通过SQL代码块模式解析")

        # 模式2: 关键词分隔
        elif any(
            keyword in text.lower()
            for keyword in ["要求:", "需求:", "requirements:", "请生成"]
        ):
            # 查找分隔关键词
            split_patterns = [
                r"要求[：:]\s*(.*?)(?=\s*(?:select|insert|update|delete|create|drop|alter|show|describe)\b)",
                r"需求[：:]\s*(.*?)(?=\s*(?:select|insert|update|delete|create|drop|alter|show|describe)\b)",
                r"请生成\s*(.*?)(?=\s*(?:select|insert|update|delete|create|drop|alter|show|describe)\b)",
                r"requirements[：:]\s*(.*?)(?=\s*(?:select|insert|update|delete|create|drop|alter|show|describe)\b)",
            ]

            for pattern in split_patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                if match:
                    requirements = match.group(1).strip()
                    # 移除要求部分，剩余部分作为SQL
                    sql = re.sub(
                        pattern, "", text, flags=re.IGNORECASE | re.DOTALL
                    ).strip()
                    confidence = 0.7
                    break

        # 模式3: SQL关键词检测
        elif self._contains_sql_keywords(text):
            # 检查是否包含明显的要求词汇
            requirement_keywords = [
                "生成",
                "创建",
                "需要",
                "要求",
                "包含",
                "模拟",
                "测试",
            ]
            lines = text.split("\n")

            sql_lines = []
            req_lines = []

            for line in lines:
                line = line.strip()
                if not line:
                    continue

                if self._is_sql_line(line):
                    sql_lines.append(line)
                elif any(keyword in line for keyword in requirement_keywords):
                    req_lines.append(line)
                else:
                    # 模糊情况，倾向于归类为SQL
                    sql_lines.append(line)

            sql = "\n".join(sql_lines).strip()
            requirements = "\n".join(req_lines).strip()
            confidence = 0.6 if req_lines else 0.8

        # 模式4: 纯SQL（默认情况）
        else:
            sql = text
            requirements = ""
            confidence = 0.5

        return ParsedRequest(sql=sql, requirements=requirements, confidence=confidence)

    async def _ai_based_parse(self, text: str) -> ParsedRequest:
        """基于AI的解析方法。

        Args:
            text: 输入文本

        Returns:
            ParsedRequest: 解析结果
        """
        try:
            prompt = f"""请从以下文本中提取SQL语句和用户要求。

输入文本：
{text}

请按照以下JSON格式返回结果：
{{
    "sql": "提取的SQL语句",
    "requirements": "用户的具体要求（如果没有则为空字符串）",
    "confidence": 置信度分数（0-1之间的数字）
}}

注意：
1. SQL语句必须是完整的、可执行的SQL
2. 用户要求是对数据生成的具体需求描述
3. 如果文本中没有明确的用户要求，requirements字段应为空字符串
4. 置信度反映解析结果的可靠程度

请只返回JSON，不要包含其他解释文字。"""

            response = await self.llm.ask([{"role": "user", "content": prompt}])

            # 尝试解析JSON响应
            import json

            # 提取JSON部分
            json_match = re.search(r"\{.*\}", response, re.DOTALL)
            if json_match:
                result_json = json.loads(json_match.group())
                return ParsedRequest(
                    sql=result_json.get("sql", "").strip(),
                    requirements=result_json.get("requirements", "").strip(),
                    confidence=float(result_json.get("confidence", 0.7)),
                )
            else:
                raise ValueError("无法从AI响应中提取JSON")

        except Exception as e:
            logger.warning(f"AI解析失败: {str(e)}")
            # 降级到规则解析
            return self._rule_based_parse(text)

    def _contains_sql_keywords(self, text: str) -> bool:
        """检查文本是否包含SQL关键词。"""
        sql_keywords = [
            "select",
            "insert",
            "update",
            "delete",
            "create",
            "drop",
            "alter",
            "show",
            "describe",
            "from",
            "where",
            "group by",
            "order by",
            "having",
            "join",
            "union",
            "values",
            "into",
            "table",
        ]

        text_lower = text.lower()
        return any(keyword in text_lower for keyword in sql_keywords)

    def _is_sql_line(self, line: str) -> bool:
        """判断一行文本是否像SQL语句。"""
        line_lower = line.lower().strip()

        # SQL起始关键词
        sql_starters = [
            "select",
            "insert",
            "update",
            "delete",
            "create",
            "drop",
            "alter",
            "show",
            "describe",
            "explain",
            "with",
        ]

        # SQL特征模式
        sql_patterns = [
            r"\bfrom\s+\w+",
            r"\bwhere\s+",
            r"\bgroup\s+by\b",
            r"\border\s+by\b",
            r"\bjoin\s+",
            r"\bunion\b",
            r"\bvalues\s*\(",
            r"[=<>!]+",
            r"\b\w+\.\w+\b",  # table.column
        ]

        # 检查是否以SQL关键词开始
        for starter in sql_starters:
            if line_lower.startswith(starter):
                return True

        # 检查是否包含SQL模式
        for pattern in sql_patterns:
            if re.search(pattern, line_lower):
                return True

        return False


# 创建全局解析器实例
_parser_instance = None


def get_text_parser() -> TextParser:
    """获取文本解析器的全局实例。"""
    global _parser_instance
    if _parser_instance is None:
        _parser_instance = TextParser()
    return _parser_instance
