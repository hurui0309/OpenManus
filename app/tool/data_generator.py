"""Data generation tool for creating test data based on SQL queries."""

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    # SQLAlchemy 2.0+
    from sqlalchemy import inspect
except ImportError:
    # SQLAlchemy 1.4
    from sqlalchemy.inspection import inspect

from sqlalchemy.exc import SQLAlchemyError

from app.config import Config
from app.datasource import DataSourceManager
from app.exceptions import DatabaseError
from app.llm import LLM
from app.prompt.data_generator import (
    DATA_GENERATOR_ASSISTANT_PROMPT,
    DATA_GENERATOR_SYSTEM_PROMPT,
    DATA_GENERATOR_USER_PROMPT,
)
from app.tool.base import BaseTool, ToolResult

logger = logging.getLogger(__name__)


class DataGeneratorTool(BaseTool):
    """数据生成工具类，用于根据SQL生成测试数据。"""

    name: str = "data_generator"
    description: str = "Generates test data based on SQL queries and table structure"
    parameters: Dict = {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SQL query to analyze for data generation",
            },
            "ds_name": {
                "type": "string",
                "description": "The datasource name to use for data generation (optional)",
            },
        },
        "required": ["sql"],
    }

    config: Config
    llm: Optional[LLM] = None
    engine: Optional[Engine] = None
    datasource_manager: Optional[DataSourceManager] = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        """初始化数据生成工具。"""
        super().__init__(**data)
        self.llm = LLM(config_name="default")
        self.engine = create_engine(self.config.database.connection_url)
        self.datasource_manager = DataSourceManager(self.config)
        # 添加实例级别的缓存
        self._schema_cache = {}
        self._next_id_cache = {}
        self._ensure_log_table()

    async def _get_target_engine(self, ds_name: Optional[str] = None) -> Engine:
        """获取目标数据源的引擎。

        Args:
            ds_name: 数据源名称，如果为空则使用默认数据源

        Returns:
            Engine: 目标数据源的引擎
        """
        if ds_name:
            return await self.datasource_manager.get_engine(ds_name)
        else:
            return self.engine

    async def _get_datasource_type(self, ds_name: Optional[str] = None) -> str:
        """获取数据源类型。

        Args:
            ds_name: 数据源名称

        Returns:
            str: 数据源类型
        """
        if ds_name:
            config = await self.datasource_manager.get_datasource_config(ds_name)
            return config.ds_type.lower()
        else:
            return self.config.database.driver.lower()

    def _ensure_log_table(self) -> None:
        """确保数据生成日志表存在。"""
        try:
            # 检查表是否存在
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()

            if "data_generation_logs" not in tables:
                # 使用事务创建日志表
                with self.engine.begin() as conn:
                    # 创建日志表
                    create_table_sql = """
                    CREATE TABLE data_generation_logs (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        sql_text TEXT NOT NULL,
                        ds_name VARCHAR(32),
                        generated_sql LONGTEXT,
                        execution_status VARCHAR(20) DEFAULT 'pending',
                        error_message TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                    conn.execute(text(create_table_sql))
                    # begin() 上下文管理器会自动提交
                logger.info("数据生成日志表创建成功")

        except SQLAlchemyError as e:
            logger.error(f"创建日志表失败: {str(e)}")
        except Exception as e:
            logger.error(f"创建日志表时出现意外错误: {str(e)}")

    def _log_generation(
        self,
        sql_text: str,
        ds_name: Optional[str],
        generated_sql: str,
        status: str,
        error_message: Optional[str] = None,
    ) -> None:
        """记录数据生成操作。"""
        try:
            # 使用事务明确管理提交
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                    INSERT INTO data_generation_logs
                    (sql_text, ds_name, generated_sql, execution_status, error_message)
                    VALUES (:sql_text, :ds_name, :generated_sql, :status, :error_message)
                    """
                    ),
                    {
                        "sql_text": sql_text,
                        "ds_name": ds_name,
                        "generated_sql": generated_sql,
                        "status": status,
                        "error_message": error_message,
                    },
                )
                # begin() 上下文管理器会自动提交
        except SQLAlchemyError as e:
            logger.error(f"记录数据生成日志失败: {str(e)}")
        except Exception as e:
            logger.error(f"记录数据生成日志时出现意外错误: {str(e)}")

    async def get_table_schema(
        self, table_name: str, ds_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """获取表结构信息，包括详细的字段约束。

        Args:
            table_name: 表名
            ds_name: 数据源名称

        Returns:
            Dict[str, Any]: 表结构信息，包括字段约束
        """
        try:
            target_engine = await self._get_target_engine(ds_name)
            ds_type = await self._get_datasource_type(ds_name)

            # 对于Hive，使用特殊方法获取表结构
            if ds_type == "hive":
                return await self._get_hive_table_schema(table_name, ds_name)
            else:
                inspector = inspect(target_engine)

                # 获取列信息
                columns = inspector.get_columns(table_name)

                # 增强列信息，获取更详细的约束
                enhanced_columns = []
                for col in columns:
                    enhanced_col = await self._enhance_column_info(
                        col, table_name, ds_type, target_engine
                    )
                    enhanced_columns.append(enhanced_col)

                # 获取主键信息
                primary_keys = inspector.get_pk_constraint(table_name)

                # 获取外键信息
                foreign_keys = inspector.get_foreign_keys(table_name)

                # 获取唯一约束
                unique_constraints = []
                try:
                    unique_constraints = inspector.get_unique_constraints(table_name)
                except Exception as e:
                    logger.debug(f"无法获取唯一约束: {e}")

                # 获取检查约束
                check_constraints = []
                try:
                    check_constraints = inspector.get_check_constraints(table_name)
                except Exception as e:
                    logger.debug(f"无法获取检查约束: {e}")

                return {
                    "table_name": table_name,
                    "columns": enhanced_columns,
                    "primary_keys": primary_keys.get("constrained_columns", []),
                    "foreign_keys": foreign_keys,
                    "unique_constraints": unique_constraints,
                    "check_constraints": check_constraints,
                    "datasource": ds_name or "default",
                    "is_partitioned": False,
                    "partition_columns": [],
                }

        except Exception as e:
            logger.error(f"获取表结构失败: {str(e)}")
            raise DatabaseError(f"获取表结构失败: {str(e)}")

    async def _get_hive_table_schema(
        self, table_name: str, ds_name: str
    ) -> Dict[str, Any]:
        """获取Hive表结构信息。

        Args:
            table_name: 表名
            ds_name: 数据源名称

        Returns:
            Dict[str, Any]: Hive表结构信息
        """
        try:
            target_engine = await self._get_target_engine(ds_name)

            with target_engine.connect() as conn:
                # 获取表结构描述
                result = conn.execute(text(f"DESCRIBE {table_name}"))
                columns = []
                partition_columns = []
                in_partition_section = False

                for row in result:
                    # 清理列名和类型，去除多余的括号和引号
                    raw_col_name = str(row[0]) if len(row) > 0 else ""
                    raw_col_type = str(row[1]) if len(row) > 1 else "string"

                    # 清理列名
                    col_name = raw_col_name.strip("('\")")
                    col_type = raw_col_type.strip("('\")")

                    logger.debug(
                        f"解析列: 原始='{raw_col_name}' 清理后='{col_name}' 类型='{col_type}'"
                    )

                    # 检查是否到达分区信息部分
                    if (
                        "# Partition Information" in raw_col_name
                        or "partition_columns" in raw_col_name.lower()
                        or "col_name" in raw_col_name.lower()
                    ):
                        in_partition_section = True
                        continue

                    # 跳过空行、注释行和标题行
                    if (
                        not col_name
                        or col_name.startswith("#")
                        or col_name in ["col_name", ""]
                        or not col_name.strip()
                    ):
                        continue

                    col_info = {
                        "name": col_name,
                        "type": col_type,
                        "nullable": True,
                        "default": None,
                    }

                    if in_partition_section:
                        partition_columns.append(col_info)
                        logger.debug(f"添加分区列: {col_name}")
                    else:
                        columns.append(col_info)
                        logger.debug(f"添加数据列: {col_name}")

            # 获取分区信息
            is_partitioned = len(partition_columns) > 0

            # 如果是分区表，获取现有分区
            existing_partitions = []
            if is_partitioned:
                existing_partitions = (
                    await self.datasource_manager.get_table_partitions(
                        table_name, ds_name
                    )
                )

            return {
                "table_name": table_name,
                "columns": columns,
                "primary_keys": [],  # Hive通常没有主键约束
                "foreign_keys": [],  # Hive通常没有外键约束
                "datasource": ds_name,
                "is_partitioned": is_partitioned,
                "partition_columns": [col["name"] for col in partition_columns],
                "existing_partitions": existing_partitions,
            }

        except Exception as e:
            logger.error(f"获取Hive表结构失败: {str(e)}")
            raise DatabaseError(f"获取Hive表结构失败: {str(e)}")

    def extract_table_names(self, sql: str) -> List[str]:
        """从SQL语句中提取表名。"""
        # 简单的正则表达式提取，可能需要更复杂的解析
        import re

        # 匹配 FROM 和 JOIN 后面的表名
        patterns = [
            r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            r"\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            r"\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            r"\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        ]

        tables = []
        for pattern in patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE)
            for match in matches:
                table_name = match.group(1)
                if table_name not in tables:
                    tables.append(table_name)

        return tables

    def _parse_generated_sql(self, response: str) -> Tuple[str, str]:
        """解析LLM生成的响应，提取SQL语句。

        Args:
            response (str): LLM生成的响应文本

        Returns:
            tuple[str, str]: (SQL语句, 错误信息)，如果解析成功则错误信息为空
        """
        try:
            if not response or not response.strip():
                error_detail = "LLM返回了空响应"
                logger.error(f"SQL解析失败: {error_detail}")
                return "", error_detail

            # 记录原始响应用于调试
            logger.info(f"开始解析LLM响应，响应长度: {len(response)} 字符")
            logger.debug(f"LLM原始响应: {response[:500]}...")

            # 🚀 新增：检测响应是否被截断
            is_truncated = self._detect_response_truncation(response)
            if is_truncated:
                logger.warning("检测到LLM响应可能被截断")

            sql_statements = []
            in_sql_block = False
            current_sql = []
            sql_block_count = 0

            lines = response.split("\n")
            logger.debug(f"响应共包含 {len(lines)} 行")

            for line_no, line in enumerate(lines, 1):
                line_stripped = line.strip()

                # 处理SQL代码块开始标记
                if line_stripped.startswith("```sql"):
                    if in_sql_block:
                        logger.warning(f"第{line_no}行: 发现嵌套的SQL代码块开始标记")
                    in_sql_block = True
                    sql_block_count += 1
                    logger.debug(f"第{line_no}行: 发现SQL代码块开始标记")
                    continue

                # 处理SQL代码块结束标记
                elif line_stripped.startswith("```") and in_sql_block:
                    in_sql_block = False
                    if current_sql:
                        sql_content = "\n".join(current_sql).strip()
                        if sql_content:
                            sql_statements.append(sql_content)
                            logger.debug(
                                f"第{line_no}行: 完成SQL代码块解析，内容长度: {len(sql_content)}"
                            )
                        current_sql = []
                    continue

                # 收集SQL语句
                if in_sql_block:
                    current_sql.append(line)

            # 处理最后一个未正确闭合的SQL块
            if current_sql:
                sql_content = "\n".join(current_sql).strip()
                if sql_content:
                    sql_statements.append(sql_content)
                    logger.warning("发现未正确闭合的SQL代码块，已自动处理")

            # 🚀 新增：如果仍在SQL块中且检测到截断，尝试修复
            if in_sql_block and is_truncated and current_sql:
                sql_content = "\n".join(current_sql).strip()
                fixed_sql = self._attempt_sql_repair(sql_content)
                if fixed_sql:
                    sql_statements.append(fixed_sql)
                    logger.info("尝试修复截断的SQL语句")

            # 检查解析结果
            logger.info(
                f"SQL解析统计: 发现 {sql_block_count} 个代码块，提取 {len(sql_statements)} 条SQL语句"
            )

            if not sql_statements:
                # 尝试从响应中直接提取可能的SQL语句
                potential_sql = self._extract_sql_fallback(response)
                if potential_sql:
                    logger.info("通过备用方法提取到SQL语句")
                    sql_statements = [potential_sql]
                else:
                    error_detail = self._build_parse_error_detail(
                        response, sql_block_count, is_truncated
                    )
                    logger.error(f"SQL解析失败: {error_detail}")
                    return "", error_detail

            # 合并所有SQL语句，确保每条语句以分号结束
            final_sql_parts = []
            for i, stmt in enumerate(sql_statements, 1):
                stmt_clean = stmt.strip()
                if stmt_clean:
                    # 🚀 新增：验证SQL完整性
                    if not self._validate_sql_completeness(stmt_clean):
                        logger.warning(f"SQL语句 {i} 可能不完整: {stmt_clean[:100]}...")
                        # 尝试修复
                        fixed_stmt = self._attempt_sql_repair(stmt_clean)
                        if fixed_stmt:
                            stmt_clean = fixed_stmt
                            logger.info(f"已尝试修复SQL语句 {i}")

                    if not stmt_clean.endswith(";"):
                        stmt_clean += ";"
                    final_sql_parts.append(stmt_clean)
                    logger.debug(f"SQL语句 {i}: {stmt_clean[:100]}...")

            final_sql = "\n".join(final_sql_parts)

            if not final_sql.strip():
                error_detail = "解析出的SQL语句为空"
                logger.error(f"SQL解析失败: {error_detail}")
                return "", error_detail

            logger.info(f"SQL解析成功，最终SQL长度: {len(final_sql)} 字符")
            return final_sql, ""

        except Exception as e:
            error_detail = f"SQL解析过程出现异常: {str(e)}"
            logger.error(f"SQL解析异常: {error_detail}", exc_info=True)
            return "", error_detail

    def _detect_response_truncation(self, response: str) -> bool:
        """检测LLM响应是否被截断。

        Args:
            response: LLM响应文本

        Returns:
            bool: True表示可能被截断
        """
        try:
            # 检查常见的截断特征
            truncation_indicators = [
                # 1. 响应以不完整的单词结束
                lambda text: len(text.strip()) > 10
                and not text.strip()[-1] in ".;,!\"'`})]",
                # 2. 响应包含未闭合的引号
                lambda text: text.count("'") % 2 == 1 or text.count('"') % 2 == 1,
                # 3. 响应包含未闭合的SQL代码块
                lambda text: text.count("```sql")
                > text.count("```\n") + text.count("``` "),
                # 4. 响应以常见的截断模式结束
                lambda text: any(
                    text.strip().endswith(pattern)
                    for pattern in ["格式", "内容", "数据", "语句", "查询"]
                ),
                # 5. VALUES语句中括号不匹配
                lambda text: "VALUES" in text.upper()
                and text.count("(") != text.count(")"),
            ]

            for indicator in truncation_indicators:
                if indicator(response):
                    return True

            return False

        except Exception as e:
            logger.debug(f"截断检测失败: {e}")
            return False

    def _validate_sql_completeness(self, sql: str) -> bool:
        """验证SQL语句的完整性。

        Args:
            sql: SQL语句

        Returns:
            bool: True表示SQL语句完整
        """
        try:
            sql_upper = sql.upper().strip()

            # 基本完整性检查
            checks = [
                # 1. 包含基本SQL关键词
                any(
                    keyword in sql_upper for keyword in ["INSERT", "REPLACE", "UPDATE"]
                ),
                # 2. VALUES语句括号匹配
                sql.count("(") == sql.count(")") if "VALUES" in sql_upper else True,
                # 3. 引号匹配
                sql.count("'") % 2 == 0,
                sql.count('"') % 2 == 0,
                # 4. 不以常见的不完整模式结束
                not any(
                    sql.strip().endswith(pattern)
                    for pattern in ["，", ",", "'", '"', "格式", "内容"]
                ),
            ]

            return all(checks)

        except Exception as e:
            logger.debug(f"SQL完整性验证失败: {e}")
            return False

    def _attempt_sql_repair(self, sql: str) -> str:
        """尝试修复不完整的SQL语句。

        Args:
            sql: 可能不完整的SQL语句

        Returns:
            str: 修复后的SQL语句，如果无法修复则返回原语句
        """
        try:
            repaired_sql = sql.strip()

            # 1. 修复未闭合的引号
            if repaired_sql.count("'") % 2 == 1:
                repaired_sql += "'"
                logger.debug("修复了未闭合的单引号")

            if repaired_sql.count('"') % 2 == 1:
                repaired_sql += '"'
                logger.debug("修复了未闭合的双引号")

            # 2. 修复未闭合的括号（针对VALUES语句）
            if "VALUES" in repaired_sql.upper():
                open_parens = repaired_sql.count("(")
                close_parens = repaired_sql.count(")")
                if open_parens > close_parens:
                    repaired_sql += ")" * (open_parens - close_parens)
                    logger.debug(f"修复了 {open_parens - close_parens} 个未闭合的括号")

            # 3. 移除可能的截断标识符
            truncation_patterns = ["格式", "内容", "数据", "语句", "查询"]
            for pattern in truncation_patterns:
                if repaired_sql.endswith(pattern):
                    repaired_sql = repaired_sql[: -len(pattern)].strip()
                    logger.debug(f"移除了截断标识符: {pattern}")

            # 4. 确保语句以分号结束
            if not repaired_sql.endswith(";"):
                repaired_sql += ";"

            return repaired_sql

        except Exception as e:
            logger.debug(f"SQL修复失败: {e}")
            return sql

    def _extract_sql_fallback(self, response: str) -> str:
        """备用SQL提取方法，尝试从响应中直接识别SQL语句。

        Args:
            response: LLM响应文本

        Returns:
            str: 提取到的SQL语句，如果没有则返回空字符串
        """
        try:
            import re

            # 常见的SQL关键词
            sql_keywords = [
                "INSERT",
                "REPLACE",
                "UPDATE",
                "DELETE",
                "CREATE",
                "DROP",
                "ALTER",
            ]

            lines = response.split("\n")
            sql_lines = []

            for line in lines:
                line_clean = line.strip()
                # 检查是否包含SQL关键词
                if any(keyword in line_clean.upper() for keyword in sql_keywords):
                    sql_lines.append(line)
                    logger.debug(f"备用方法发现可能的SQL行: {line_clean[:50]}...")

            if sql_lines:
                potential_sql = "\n".join(sql_lines).strip()
                logger.info(f"备用方法提取到 {len(sql_lines)} 行SQL内容")
                return potential_sql

        except Exception as e:
            logger.debug(f"备用SQL提取方法失败: {e}")

        return ""

    def _fix_markdown_format(self, text: str) -> str:
        """修复markdown格式问题，确保代码块完整性。

        Args:
            text: 原始文本

        Returns:
            str: 修复后的文本
        """
        try:
            import re

            fixed_text = text

            # 1. 检查并修复未闭合的SQL代码块
            sql_blocks = re.finditer(r"```sql\b.*?(?=```|$)", fixed_text, re.DOTALL)
            blocks_to_fix = []

            for match in sql_blocks:
                block_content = match.group(0)
                # 检查是否有结尾的 ```
                if not block_content.rstrip().endswith("```"):
                    blocks_to_fix.append(match)

            # 从后往前修复，避免位置偏移
            for match in reversed(blocks_to_fix):
                start, end = match.span()
                block_content = match.group(0).rstrip()

                # 添加缺失的结尾标记
                if not block_content.endswith("```"):
                    # 检查是否需要添加换行
                    if not block_content.endswith("\n"):
                        block_content += "\n"
                    block_content += "```"

                    fixed_text = fixed_text[:start] + block_content + fixed_text[end:]
                    logger.debug(f"修复了SQL代码块: 位置 {start}-{end}")

            # 2. 检查并修复其他类型的代码块
            general_blocks = re.finditer(r"```\w*.*?(?=```|$)", fixed_text, re.DOTALL)
            blocks_to_fix = []

            for match in general_blocks:
                block_content = match.group(0)
                # 跳过已经正确闭合的块
                if block_content.count("```") >= 2:
                    continue
                blocks_to_fix.append(match)

            # 从后往前修复
            for match in reversed(blocks_to_fix):
                start, end = match.span()
                block_content = match.group(0).rstrip()

                if not block_content.endswith("```"):
                    if not block_content.endswith("\n"):
                        block_content += "\n"
                    block_content += "```"

                    fixed_text = fixed_text[:start] + block_content + fixed_text[end:]
                    logger.debug(f"修复了代码块: 位置 {start}-{end}")

            # 3. 清理可能的截断内容标识
            truncation_patterns = [
                r"\s*格式\s*$",
                r"\s*内容\s*$",
                r"\s*数据\s*$",
                r"\s*语句\s*$",
                r"\s*查询\s*$",
            ]

            for pattern in truncation_patterns:
                if re.search(pattern, fixed_text):
                    fixed_text = re.sub(pattern, "", fixed_text).rstrip()
                    logger.debug(f"移除了截断标识符: {pattern}")

            # 4. 确保文本以换行结尾
            if fixed_text and not fixed_text.endswith("\n"):
                fixed_text += "\n"

            return fixed_text

        except Exception as e:
            logger.warning(f"Markdown格式修复失败: {e}")
            return text

    def _build_parse_error_detail(
        self, response: str, sql_block_count: int, is_truncated: bool
    ) -> str:
        """构建详细的解析错误信息。

        Args:
            response: LLM响应文本
            sql_block_count: 发现的SQL代码块数量
            is_truncated: 是否检测到截断

        Returns:
            str: 详细的错误信息
        """
        error_parts = ["LLM未生成有效的SQL语句"]

        # 分析响应内容
        response_length = len(response.strip())
        response_lines = len(response.split("\n"))

        error_parts.append(
            f"响应分析: 长度={response_length}字符, 行数={response_lines}"
        )

        if sql_block_count == 0:
            error_parts.append("❌ 未发现```sql代码块")
            # 检查是否有其他类型的代码块
            if "```" in response:
                import re

                code_blocks = re.findall(r"```(\w*)", response)
                if code_blocks:
                    error_parts.append(f"发现其他代码块: {', '.join(set(code_blocks))}")
        else:
            error_parts.append(
                f"✓ 发现{sql_block_count}个```sql代码块，但提取的SQL内容为空"
            )

        # 检查是否包含常见的SQL关键词
        sql_keywords = ["INSERT", "REPLACE", "UPDATE", "SELECT", "CREATE"]
        found_keywords = [kw for kw in sql_keywords if kw in response.upper()]
        if found_keywords:
            error_parts.append(f"发现SQL关键词: {', '.join(found_keywords)}")
        else:
            error_parts.append("❌ 未发现常见SQL关键词")

        # 截取响应的开头和结尾用于诊断
        preview_length = 200
        if response_length > preview_length * 2:
            preview = f"响应开头: {response[:preview_length]}...响应结尾: ...{response[-preview_length:]}"
        else:
            preview = f"完整响应: {response}"

        error_parts.append(f"响应内容预览: {preview}")

        if is_truncated:
            error_parts.append("❌ 检测到LLM响应可能被截断")
            error_parts.append("💡 建议: 尝试重新生成或简化数据要求")

        return " | ".join(error_parts)

    def _clean_sql_for_hive_execution(self, sql: str) -> str:
        """清理SQL语句以适配Hive执行。

        Args:
            sql: 原始SQL语句

        Returns:
            str: 清理后的SQL语句
        """
        import re

        # 基本清理
        cleaned_sql = sql.strip()

        # 移除SQL注释（-- 和 /* */ 样式）
        cleaned_sql = re.sub(r"--.*?(?:\n|$)", " ", cleaned_sql)
        cleaned_sql = re.sub(r"/\*.*?\*/", " ", cleaned_sql, flags=re.DOTALL)

        # 移除多余的空白字符
        cleaned_sql = re.sub(r"\s+", " ", cleaned_sql).strip()

        logger.debug(f"SQL清理: 原始长度={len(sql)} 清理后长度={len(cleaned_sql)}")

        return cleaned_sql

    def _split_sql_statements(self, sql: str) -> List[str]:
        """智能分割SQL语句。

        Args:
            sql: 包含多个SQL语句的字符串

        Returns:
            List[str]: 分割后的SQL语句列表
        """
        import re

        # 先清理注释
        cleaned_sql = self._clean_sql_for_hive_execution(sql)

        # 使用正则表达式分割SQL语句，更准确地处理分号
        # 这个正则表达式匹配不在引号内的分号
        statements = []
        current_pos = 0
        in_single_quote = False
        in_double_quote = False
        paren_count = 0

        i = 0
        while i < len(cleaned_sql):
            char = cleaned_sql[i]

            # 处理引号
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
            # 处理括号
            elif not in_single_quote and not in_double_quote:
                if char == "(":
                    paren_count += 1
                elif char == ")":
                    paren_count -= 1
                # 处理分号
                elif char == ";" and paren_count == 0:
                    # 找到语句边界
                    stmt = cleaned_sql[current_pos:i].strip()
                    if stmt:
                        statements.append(stmt)
                    current_pos = i + 1

            i += 1

        # 处理最后一个语句
        if current_pos < len(cleaned_sql):
            stmt = cleaned_sql[current_pos:].strip()
            if stmt and not stmt.endswith(";"):
                statements.append(stmt)

        # 过滤空语句
        statements = [stmt for stmt in statements if stmt.strip()]

        logger.debug(f"SQL分割结果: 找到 {len(statements)} 条语句")
        for i, stmt in enumerate(statements, 1):
            logger.debug(f"语句 {i}: {stmt[:50]}...")

        return statements

    async def execute_data_generation(
        self, sql: str, ds_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """执行数据生成SQL语句。

        Args:
            sql (str): 要执行的SQL语句
            ds_name: 目标数据源名称

        Returns:
            Dict[str, Any]: 执行结果，包含影响的行数等信息

        Raises:
            DatabaseError: 当SQL执行失败时抛出
        """
        if not sql.strip():
            raise DatabaseError("Empty SQL statement")

        try:
            target_engine = await self._get_target_engine(ds_name)
            ds_type = await self._get_datasource_type(ds_name)

            # 智能分割SQL语句
            statements = self._split_sql_statements(sql)

            if not statements:
                raise DatabaseError("No valid SQL statements found")

            logger.info(f"找到 {len(statements)} 条SQL语句待执行")

            # 初始化执行结果统计
            execution_results = {
                "total_statements": len(statements),
                "successful_statements": 0,
                "total_rows_affected": 0,
                "statement_details": [],
                "affected_tables": set(),
            }

            # 使用连接而不是事务，因为Hive可能不支持事务
            with target_engine.connect() as conn:
                # 对于Hive，先设置必要的参数
                if ds_type == "hive":
                    # 设置动态分区参数
                    conn.execute(text("SET hive.exec.dynamic.partition = true"))
                    conn.execute(
                        text("SET hive.exec.dynamic.partition.mode = nonstrict")
                    )

                # 执行每条SQL语句
                for i, stmt in enumerate(statements, 1):
                    try:
                        # 清理单个语句
                        if ds_type == "hive":
                            stmt = self._clean_sql_for_hive_execution(stmt)

                        logger.info(f"执行第 {i}/{len(statements)} 条SQL语句")
                        logger.debug(f"SQL: {stmt[:200]}...")

                        # 预估影响行数（用于INSERT VALUES语句）
                        estimated_rows = self._estimate_affected_rows(stmt)

                        # 提取影响的表名
                        affected_table = self._extract_table_from_statement(stmt)

                        # 执行SQL语句
                        result = conn.execute(text(stmt))

                        # 获取实际影响的行数
                        actual_rows = getattr(result, "rowcount", estimated_rows)

                        # 如果数据库不返回rowcount，使用预估值
                        if actual_rows == -1 or actual_rows is None:
                            actual_rows = estimated_rows

                        # 更新统计信息
                        execution_results["successful_statements"] += 1
                        execution_results["total_rows_affected"] += actual_rows

                        if affected_table:
                            execution_results["affected_tables"].add(affected_table)

                        # 记录单条语句详情
                        execution_results["statement_details"].append(
                            {
                                "statement_index": i,
                                "sql_preview": (
                                    stmt[:100] + "..." if len(stmt) > 100 else stmt
                                ),
                                "affected_table": affected_table,
                                "rows_affected": actual_rows,
                                "status": "success",
                            }
                        )

                        logger.info(f"SQL语句 {i} 执行成功，影响 {actual_rows} 行")

                    except SQLAlchemyError as e:
                        logger.error(
                            f"Failed to execute SQL statement {i}: {stmt[:100]}..."
                        )
                        logger.error(f"Error: {str(e)}")

                        # 记录失败的语句
                        execution_results["statement_details"].append(
                            {
                                "statement_index": i,
                                "sql_preview": (
                                    stmt[:100] + "..." if len(stmt) > 100 else stmt
                                ),
                                "affected_table": self._extract_table_from_statement(
                                    stmt
                                ),
                                "rows_affected": 0,
                                "status": "failed",
                                "error": str(e),
                            }
                        )

                        raise DatabaseError(
                            f"Failed to execute SQL statement {i}: {str(e)}\nSQL: {stmt[:200]}..."
                        )

            # 转换set为list用于JSON序列化
            execution_results["affected_tables"] = list(
                execution_results["affected_tables"]
            )

            logger.info(
                f"数据生成完成: 成功执行 {execution_results['successful_statements']}/{execution_results['total_statements']} 条SQL，共影响 {execution_results['total_rows_affected']} 行"
            )

            return execution_results

        except SQLAlchemyError as e:
            logger.error(f"Database operation failed: {str(e)}")
            raise DatabaseError(f"Database operation failed: {str(e)}")

    async def execute_stream(self, **kwargs):
        """执行数据生成任务的流式版本。

        Args:
            **kwargs: 包含sql和ds_name参数

        Yields:
            dict: 流式消息内容
        """
        sql = kwargs.get("sql")
        ds_name = kwargs.get("ds_name")

        if not sql:
            yield {
                "content": "❌ **错误**: 缺少必需参数 sql\n\n",
                "role": "assistant",
                "type": "error",
            }
            return

        try:
            # 开始数据生成分析
            yield {
                "content": "🛠️ **正在准备数据生成分析...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 提取表名
            table_names = self.extract_table_names(sql)
            if not table_names:
                yield {
                    "content": "❌ **错误**: 无法从SQL中提取表名\n\n",
                    "role": "assistant",
                    "type": "error",
                }
                return

            yield {
                "content": f"📋 **检测到表**: {', '.join(table_names)}\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 获取数据源类型
            ds_type = await self._get_datasource_type(ds_name)

            yield {
                "content": f"📊 **数据源类型**: {ds_type.upper()}\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 🚀 并发获取表结构信息
            yield {
                "content": "🗂️ **正在并发获取所有表结构信息...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            table_schemas = await self._get_table_schema_batch(table_names, ds_name)

            if not table_schemas:
                yield {
                    "content": "❌ **错误**: 无法获取任何表的结构信息\n\n",
                    "role": "assistant",
                    "type": "error",
                }
                return

            for table_name in table_names:
                if table_name in table_schemas:
                    yield {
                        "content": f"  ✅ 表 `{table_name}` 结构获取成功\n\n",
                        "role": "assistant",
                        "type": "text",
                    }
                else:
                    yield {
                        "content": f"  ⚠️ 表 `{table_name}` 结构获取失败\n\n",
                        "role": "assistant",
                        "type": "text",
                    }

            # 🚀 并发获取主键范围信息
            yield {
                "content": "🔍 **正在优化主键范围分析...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            next_ids = await self._get_batch_next_ids(table_schemas, ds_name)

            # 🚀 构建简化的结构信息
            schema_info = self._build_simplified_schema_info(
                table_schemas, next_ids, ds_type
            )

            # 根据数据源类型构建简化的提示词
            if ds_type == "mysql":
                specific_instructions = """
                **MySQL要求**:
                - 使用 `REPLACE INTO` 避免主键冲突
                - 支持批量插入
                """
            elif ds_type == "hive":
                specific_instructions = """
                **Hive要求**:
                - 分区表语法: INSERT INTO TABLE table_name PARTITION(col='value') VALUES (...)
                - VALUES中只包含数据列，不含分区列
                """
            else:
                specific_instructions = """
                **标准SQL要求**:
                - 使用提供的建议起始ID避免主键冲突
                - 支持批量插入
                """

            # 🚀 构建简化的提示词 - 针对截断问题优化
            prompt = f"""基于以下信息生成测试数据SQL：

原SQL: {sql}
数据源: {ds_name or 'default'} ({ds_type})

表结构:
{schema_info}

{specific_instructions}

要求:
1. 生成3-5条测试记录（避免过长）
2. 遵循数据库约束
3. 使用建议的起始ID
4. 使用简洁的中文值（如：'用户1'、'测试数据'）
5. 返回完整的SQL代码块

格式要求：
```sql
-- 您的SQL语句
```

请直接生成INSERT语句："""

            # 🚀 开始流式调用LLM (简化版)
            yield {
                "content": "🤖 **正在生成测试数据SQL...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 流式调用LLM并实时输出
            full_response = ""
            async for chunk in self.llm.ask_stream(
                [{"role": "user", "content": prompt}]
            ):
                if chunk:
                    full_response += chunk
                    yield {
                        "content": chunk,
                        "role": "assistant",
                        "type": "llm_stream",
                    }

            # 后处理和保存结果
            yield {
                "content": "\n\n---\n\n✅ **SQL生成完成，正在解析...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 解析生成的SQL
            generated_sql, error_detail = self._parse_generated_sql(full_response)

            if not generated_sql:
                # 构建详细的错误信息
                detailed_error_msg = (
                    f"❌ **SQL解析失败**\n\n**详细信息**: {error_detail}\n\n"
                )

                # 🚀 新增：修复响应预览的markdown格式
                response_preview = (
                    full_response[:300] if len(full_response) > 300 else full_response
                )
                # 确保代码块格式完整
                fixed_preview = self._fix_markdown_format(
                    f"```\n{response_preview}\n```"
                )
                detailed_error_msg += f"**LLM响应预览**: \n{fixed_preview}\n\n"

                # 添加排查建议
                detailed_error_msg += "**排查建议**:\n"
                detailed_error_msg += "1. 检查LLM是否正确使用了```sql代码块格式\n"
                detailed_error_msg += "2. 确认生成的内容包含有效的INSERT/REPLACE语句\n"
                detailed_error_msg += "3. 如果问题持续，请联系技术支持\n\n"

                # 记录详细的错误日志
                self._log_generation(sql, ds_name, "", "failed", error_detail)
                logger.error(f"SQL解析失败详情: {error_detail}")
                logger.debug(f"完整LLM响应: {full_response}")

                yield {
                    "content": detailed_error_msg,
                    "role": "assistant",
                    "type": "error",
                }
                return

            # 🚀 简化验证过程 - 只做基本检查
            validated_sql = generated_sql

            # 执行验证后的数据生成SQL
            yield {
                "content": "🚀 **开始执行数据生成...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 执行数据生成并获取详细结果
            execution_result = await self.execute_data_generation(
                validated_sql, ds_name
            )

            # 记录成功日志
            self._log_generation(sql, ds_name, validated_sql, "success")

            # 清理缓存中的next_id（避免重复使用相同ID）
            self._next_id_cache.clear()

            # 显示详细的写入完成信息
            success_content = self._format_execution_result(execution_result)
            yield {
                "content": success_content,
                "role": "assistant",
                "type": "summary",
            }

            # 结束标记
            yield {
                "content": "[DONE]",
                "role": "assistant",
                "type": "done",
            }

        except DatabaseError as e:
            error_msg = str(e)
            used_sql = locals().get("validated_sql") or locals().get(
                "generated_sql", ""
            )
            self._log_generation(sql, ds_name, used_sql, "failed", error_msg)

            # 简化错误处理
            detailed_error = f"❌ **数据生成失败**: {error_msg}\n\n"

            if "duplicate" in error_msg.lower():
                detailed_error += "💡 建议: 主键冲突，请重试\n\n"
            elif "null" in error_msg.lower():
                detailed_error += "💡 建议: NOT NULL约束违反\n\n"

            yield {
                "content": detailed_error,
                "role": "assistant",
                "type": "error",
            }

        except Exception as e:
            error_msg = f"数据生成失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            yield {
                "content": f"❌ **系统错误**: {error_msg}\n\n",
                "role": "assistant",
                "type": "error",
            }

    async def execute(self, **kwargs) -> ToolResult:
        """执行数据生成任务。

        Args:
            **kwargs: 包含sql和ds_name参数

        Returns:
            ToolResult: 工具执行结果
        """
        sql = kwargs.get("sql")
        ds_name = kwargs.get("ds_name")

        if not sql:
            return ToolResult(error="Missing required parameter: sql")

        try:
            # 提取表名
            table_names = self.extract_table_names(sql)

            if not table_names:
                return ToolResult(error="无法从SQL中提取表名")

            logger.info(f"开始处理 {len(table_names)} 个表的数据生成: {table_names}")

            # 获取数据源类型
            ds_type = await self._get_datasource_type(ds_name)

            # 🚀 并发获取所有表的结构信息
            table_schemas = await self._get_table_schema_batch(table_names, ds_name)

            if not table_schemas:
                return ToolResult(error="无法获取任何表的结构信息")

            # 🚀 并发获取所有表的下一个可用ID
            next_ids = await self._get_batch_next_ids(table_schemas, ds_name)

            # 🚀 构建简化的结构信息，减少LLM处理负担
            schema_info = self._build_simplified_schema_info(
                table_schemas, next_ids, ds_type
            )

            # 根据数据源类型构建简化的提示词
            if ds_type == "mysql":
                specific_instructions = """
                **MySQL要求**:
                - 使用 `REPLACE INTO` 避免主键冲突
                - 支持批量插入
                """
            elif ds_type == "hive":
                specific_instructions = """
                **Hive要求**:
                - 分区表语法: INSERT INTO TABLE table_name PARTITION(col='value') VALUES (...)
                - VALUES中只包含数据列，不含分区列
                """
            else:
                specific_instructions = """
                **标准SQL要求**:
                - 使用提供的建议起始ID避免主键冲突
                - 支持批量插入
                """

            # 🚀 构建简化的提示词 - 针对截断问题优化
            prompt = f"""基于以下信息生成测试数据SQL：

原SQL: {sql}
数据源: {ds_name or 'default'} ({ds_type})

表结构:
{schema_info}

{specific_instructions}

要求:
1. 生成3-5条测试记录（避免过长）
2. 遵循数据库约束
3. 使用建议的起始ID
4. 使用简洁的中文值（如：'用户1'、'测试数据'）
5. 返回完整的SQL代码块

格式要求：
```sql
-- 您的SQL语句
```

请直接生成INSERT语句："""

            # 调用LLM生成SQL
            response = await self.llm.ask([{"role": "user", "content": prompt}])

            # 🚀 新增：修复响应的markdown格式
            response = self._fix_markdown_format(response)

            # 解析生成的SQL
            generated_sql, error_detail = self._parse_generated_sql(response)

            if not generated_sql:
                # 构建详细的错误信息
                detailed_error_msg = f"SQL解析失败: {error_detail}"

                # 记录详细的错误日志
                self._log_generation(sql, ds_name, "", "failed", error_detail)
                logger.error(f"SQL解析失败详情: {error_detail}")
                logger.debug(f"完整LLM响应: {response}")

                return ToolResult(error=detailed_error_msg)

            # 简化验证过程 - 只做基本检查
            validated_sql = generated_sql

            # 执行数据生成SQL并获取详细结果
            execution_result = await self.execute_data_generation(
                validated_sql, ds_name
            )

            # 记录成功日志
            self._log_generation(sql, ds_name, validated_sql, "success")

            # 清理缓存中的next_id（避免重复使用相同ID）
            self._next_id_cache.clear()

            # 添加详细的执行结果信息
            success_info = self._format_execution_result(
                execution_result, detailed=False
            )
            response += f"\n{success_info}"

            return ToolResult(output=response)

        except DatabaseError as e:
            error_msg = str(e)
            used_sql = locals().get("validated_sql") or locals().get(
                "generated_sql", ""
            )
            self._log_generation(sql, ds_name, used_sql, "failed", error_msg)

            # 简化错误处理
            detailed_error = f"数据生成失败: {error_msg}"

            if "duplicate" in error_msg.lower():
                detailed_error += "\n\n💡 建议: 主键冲突，请重试"
            elif "null" in error_msg.lower():
                detailed_error += "\n\n💡 建议: NOT NULL约束违反"

            logger.error(f"Data generation failed: {error_msg}")
            return ToolResult(error=detailed_error)

        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self._log_generation(sql, ds_name, "", "failed", error_msg)
            logger.error(f"Unexpected error in data generation: {error_msg}")
            return ToolResult(error=error_msg)

    async def _get_next_available_id(
        self, table_name: str, id_column: str, ds_name: Optional[str] = None
    ) -> int:
        """获取下一个可用的主键值。

        Args:
            table_name: 表名
            id_column: 主键列名
            ds_name: 数据源名称

        Returns:
            int: 下一个可用的主键值
        """
        try:
            target_engine = await self._get_target_engine(ds_name)

            with target_engine.connect() as conn:
                # 查询当前最大主键值
                result = conn.execute(
                    text(f"SELECT MAX({id_column}) as max_id FROM {table_name}")
                )
                row = result.fetchone()
                max_id = row[0] if row and row[0] is not None else 0

                # 返回下一个可用的ID，从1000开始以避免冲突
                return max(max_id + 1, 1000)

        except Exception as e:
            logger.warning(f"无法获取表 {table_name} 的最大ID，使用默认值1000: {e}")
            return 1000

    async def _get_table_info_for_llm(
        self, table_name: str, ds_name: Optional[str] = None
    ) -> str:
        """获取表信息，包括现有数据的主键范围，用于LLM生成不冲突的数据。

        Args:
            table_name: 表名
            ds_name: 数据源名称

        Returns:
            str: 表信息字符串
        """
        try:
            schema = await self.get_table_schema(table_name, ds_name)
            table_info = f"表 {schema['table_name']}:\n"

            # 区分分区列和非分区列
            if schema.get("is_partitioned", False):
                # 获取非分区列
                non_partition_columns = [
                    col
                    for col in schema["columns"]
                    if col["name"] not in schema.get("partition_columns", [])
                ]

                table_info += f"数据列: {[col['name'] + '(' + str(col['type']) + ')' for col in non_partition_columns]}\n"
                table_info += (
                    f"**重要**: 这是分区表，分区列: {schema['partition_columns']}\n"
                )
                table_info += f"**注意**: INSERT语句只需要包含数据列，分区列在PARTITION子句中指定\n"

                if schema.get("existing_partitions"):
                    table_info += f"现有分区示例: {schema['existing_partitions'][:3]}\n"
            else:
                table_info += f"列: {[col['name'] + '(' + str(col['type']) + ')' for col in schema['columns']]}\n"

            table_info += f"主键: {schema['primary_keys']}\n"

            # 获取主键列的下一个可用ID（仅对非Hive数据库）
            if schema["primary_keys"]:
                ds_type = await self._get_datasource_type(ds_name)
                if ds_type != "hive":  # Hive通常不使用主键
                    primary_key = schema["primary_keys"][0]  # 假设只有一个主键
                    next_id = await self._get_next_available_id(
                        table_name, primary_key, ds_name
                    )
                    table_info += f"下一个可用主键值: {next_id}\n"

            return table_info

        except Exception as e:
            logger.warning(f"获取表 {table_name} 信息失败: {e}")
            return f"表 {table_name}: 无法获取详细信息"

    async def _enhance_column_info(
        self, col: Dict[str, Any], table_name: str, ds_type: str, engine: Engine
    ) -> Dict[str, Any]:
        """增强列信息，获取详细的约束信息。

        Args:
            col: 基础列信息
            table_name: 表名
            ds_type: 数据源类型
            engine: 数据库引擎

        Returns:
            Dict[str, Any]: 增强的列信息
        """
        enhanced_col = col.copy()

        # 获取字段长度限制
        col_type = str(col.get("type", "")).upper()
        max_length = None
        precision = None
        scale = None

        # 解析字段类型中的长度信息
        import re

        # VARCHAR(255), CHAR(10) 等
        varchar_match = re.search(r"(VAR)?CHAR\((\d+)\)", col_type)
        if varchar_match:
            max_length = int(varchar_match.group(2))

        # DECIMAL(10,2), NUMERIC(8,3) 等
        decimal_match = re.search(r"(DECIMAL|NUMERIC)\((\d+),\s*(\d+)\)", col_type)
        if decimal_match:
            precision = int(decimal_match.group(2))
            scale = int(decimal_match.group(3))

        # TEXT 类型的默认长度限制
        if "TEXT" in col_type:
            if "TINYTEXT" in col_type:
                max_length = 255
            elif "MEDIUMTEXT" in col_type:
                max_length = 16777215
            elif "LONGTEXT" in col_type:
                max_length = 4294967295
            else:  # TEXT
                max_length = 65535

        # 为不同数据源设置默认值
        if ds_type == "mysql":
            # MySQL 特定的字段长度处理
            if "INT" in col_type and "TINYINT" in col_type:
                enhanced_col["min_value"] = -128 if not "UNSIGNED" in col_type else 0
                enhanced_col["max_value"] = 127 if not "UNSIGNED" in col_type else 255
            elif "INT" in col_type and "SMALLINT" in col_type:
                enhanced_col["min_value"] = -32768 if not "UNSIGNED" in col_type else 0
                enhanced_col["max_value"] = (
                    32767 if not "UNSIGNED" in col_type else 65535
                )
            elif "INT" in col_type and "BIGINT" in col_type:
                enhanced_col["min_value"] = (
                    -9223372036854775808 if not "UNSIGNED" in col_type else 0
                )
                enhanced_col["max_value"] = (
                    9223372036854775807
                    if not "UNSIGNED" in col_type
                    else 18446744073709551615
                )
            elif "INT" in col_type:  # Regular INT
                enhanced_col["min_value"] = (
                    -2147483648 if not "UNSIGNED" in col_type else 0
                )
                enhanced_col["max_value"] = (
                    2147483647 if not "UNSIGNED" in col_type else 4294967295
                )

        elif ds_type == "postgresql":
            # PostgreSQL 特定处理
            if "SERIAL" in col_type or "BIGSERIAL" in col_type:
                enhanced_col["is_auto_increment"] = True
            if "INTEGER" in col_type:
                enhanced_col["min_value"] = -2147483648
                enhanced_col["max_value"] = 2147483647
            elif "BIGINT" in col_type:
                enhanced_col["min_value"] = -9223372036854775808
                enhanced_col["max_value"] = 9223372036854775807
            elif "SMALLINT" in col_type:
                enhanced_col["min_value"] = -32768
                enhanced_col["max_value"] = 32767

        # 添加增强的字段信息
        enhanced_col.update(
            {
                "max_length": max_length,
                "precision": precision,
                "scale": scale,
                "is_primary_key": False,  # 稍后会更新
                "is_foreign_key": False,  # 稍后会更新
                "is_unique": False,  # 稍后会更新
                "has_default": col.get("default") is not None,
                "auto_increment": col.get("autoincrement", False)
                or enhanced_col.get("is_auto_increment", False),
            }
        )

        # 尝试获取更详细的约束信息
        try:
            with engine.connect() as conn:
                col_name = col["name"]

                if ds_type == "mysql":
                    # MySQL: 查询 INFORMATION_SCHEMA 获取详细约束
                    constraint_query = text(
                        """
                        SELECT
                            COLUMN_NAME,
                            IS_NULLABLE,
                            CHARACTER_MAXIMUM_LENGTH,
                            NUMERIC_PRECISION,
                            NUMERIC_SCALE,
                            COLUMN_DEFAULT,
                            EXTRA
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = :table_name
                        AND COLUMN_NAME = :col_name
                        AND TABLE_SCHEMA = DATABASE()
                    """
                    )
                    result = conn.execute(
                        constraint_query,
                        {"table_name": table_name, "col_name": col_name},
                    )
                    row = result.fetchone()

                    if row:
                        enhanced_col["nullable"] = row[1] == "YES"
                        if row[2]:  # CHARACTER_MAXIMUM_LENGTH
                            enhanced_col["max_length"] = int(row[2])
                        if row[3]:  # NUMERIC_PRECISION
                            enhanced_col["precision"] = int(row[3])
                        if row[4]:  # NUMERIC_SCALE
                            enhanced_col["scale"] = int(row[4])
                        enhanced_col["has_default"] = row[5] is not None
                        enhanced_col["auto_increment"] = (
                            "auto_increment" in str(row[6]).lower()
                        )

                elif ds_type == "postgresql":
                    # PostgreSQL: 查询系统表获取约束信息
                    constraint_query = text(
                        """
                        SELECT
                            column_name,
                            is_nullable,
                            character_maximum_length,
                            numeric_precision,
                            numeric_scale,
                            column_default
                        FROM information_schema.columns
                        WHERE table_name = :table_name
                        AND column_name = :col_name
                        AND table_catalog = current_database()
                    """
                    )
                    result = conn.execute(
                        constraint_query,
                        {"table_name": table_name, "col_name": col_name},
                    )
                    row = result.fetchone()

                    if row:
                        enhanced_col["nullable"] = row[1] == "YES"
                        if row[2]:  # character_maximum_length
                            enhanced_col["max_length"] = int(row[2])
                        if row[3]:  # numeric_precision
                            enhanced_col["precision"] = int(row[3])
                        if row[4]:  # numeric_scale
                            enhanced_col["scale"] = int(row[4])
                        enhanced_col["has_default"] = row[5] is not None
                        enhanced_col["auto_increment"] = (
                            "nextval" in str(row[5]).lower() if row[5] else False
                        )

        except Exception as e:
            logger.debug(f"无法获取列 {col['name']} 的详细约束信息: {e}")

        return enhanced_col

    async def _validate_and_fix_generated_sql(
        self, sql: str, table_schemas: List[Dict[str, Any]], ds_type: str
    ) -> Tuple[str, List[str]]:
        """验证和修复生成的SQL，确保数据符合约束要求。

        Args:
            sql: 生成的SQL语句
            table_schemas: 表结构信息列表
            ds_type: 数据源类型

        Returns:
            Tuple[str, List[str]]: (修复后的SQL, 警告信息列表)
        """
        warnings = []

        try:
            # 解析SQL语句
            statements = self._split_sql_statements(sql)
            fixed_statements = []

            for stmt in statements:
                fixed_stmt, stmt_warnings = await self._fix_single_statement(
                    stmt, table_schemas, ds_type
                )
                # 确保语句以分号结尾
                if fixed_stmt.strip() and not fixed_stmt.strip().endswith(";"):
                    fixed_stmt = fixed_stmt.strip() + ";"
                fixed_statements.append(fixed_stmt)
                warnings.extend(stmt_warnings)

            return "\n".join(fixed_statements), warnings

        except Exception as e:
            logger.warning(f"SQL验证修复过程中出现错误: {e}")
            return sql, [f"SQL验证失败: {str(e)}"]

    async def _fix_single_statement(
        self, stmt: str, table_schemas: List[Dict[str, Any]], ds_type: str
    ) -> Tuple[str, List[str]]:
        """修复单个SQL语句。

        Args:
            stmt: SQL语句
            table_schemas: 表结构信息
            ds_type: 数据源类型

        Returns:
            Tuple[str, List[str]]: (修复后的语句, 警告列表)
        """
        warnings = []

        # 基本的SQL清理
        stmt = stmt.strip()
        if not stmt:
            return stmt, warnings

        # 提取表名和分析INSERT语句
        import re

        # 匹配 INSERT INTO 或 REPLACE INTO 语句
        insert_match = re.search(
            r"(INSERT|REPLACE)\s+INTO\s+(?:TABLE\s+)?([a-zA-Z_][a-zA-Z0-9_]*)",
            stmt,
            re.IGNORECASE,
        )

        if not insert_match:
            return stmt, warnings

        table_name = insert_match.group(2)

        # 查找对应的表结构
        table_schema = None
        for schema in table_schemas:
            if schema["table_name"].lower() == table_name.lower():
                table_schema = schema
                break

        if not table_schema:
            warnings.append(f"未找到表 {table_name} 的结构信息")
            return stmt, warnings

        # 解析SQL中指定的列
        columns_match = re.search(r"\(\s*([^)]+)\s*\)\s+VALUES", stmt, re.IGNORECASE)

        if columns_match:
            # SQL中明确指定了列
            specified_columns_str = columns_match.group(1)
            specified_column_names = [
                col.strip().strip('`"[]') for col in specified_columns_str.split(",")
            ]

            # 创建只包含指定列的临时表结构
            temp_schema = {"table_name": table_schema["table_name"], "columns": []}

            for col_name in specified_column_names:
                for col in table_schema["columns"]:
                    if col["name"].lower() == col_name.lower():
                        temp_schema["columns"].append(col)
                        break
                else:
                    warnings.append(f"未找到列 {col_name} 的定义")

            # 使用指定的列进行验证
            effective_schema = temp_schema
        else:
            # 没有指定列，使用所有非自增列
            effective_schema = {
                "table_name": table_schema["table_name"],
                "columns": [
                    col
                    for col in table_schema["columns"]
                    if not col.get("auto_increment", False)
                ],
            }

        # 验证和修复VALUES子句中的数据
        # 修改正则表达式以匹配完整的VALUES子句（包含多个值组）
        values_match = re.search(
            r"VALUES\s*(.+?)(?:;|$)", stmt, re.IGNORECASE | re.DOTALL
        )
        if values_match:
            values_content = values_match.group(1).strip()

            # 简单验证：如果VALUES内容看起来已经格式良好，且没有明显错误，则跳过修复
            if self._is_values_well_formed(values_content):
                logger.debug("VALUES 子句格式良好，跳过修复")
                return stmt, warnings

            fixed_values, value_warnings = self._fix_values_data(
                values_content, effective_schema, ds_type
            )
            warnings.extend(value_warnings)

            # 替换修复后的VALUES
            stmt = re.sub(
                r"VALUES\s*.+?(?=;|$)",
                f"VALUES {fixed_values}",
                stmt,
                flags=re.IGNORECASE | re.DOTALL,
            )

        return stmt, warnings

    def _fix_values_data(
        self, values_content: str, table_schema: Dict[str, Any], ds_type: str
    ) -> Tuple[str, List[str]]:
        """修复VALUES子句中的数据，确保符合字段约束。

        Args:
            values_content: VALUES子句内容
            table_schema: 表结构信息
            ds_type: 数据源类型

        Returns:
            Tuple[str, List[str]]: (修复后的VALUES内容, 警告列表)
        """
        warnings = []

        try:
            import re

            # 使用正则表达式更准确地解析 VALUES 子句
            # 匹配形如 (value1, value2, ...) 的模式
            pattern = r"\([^)]+\)"
            matches = re.findall(pattern, values_content, re.DOTALL)

            if not matches:
                # 如果没有找到括号，可能是单行格式
                warnings.append("未找到标准的 VALUES 格式，尝试直接处理")
                return values_content, warnings

            # 修复每个值组
            fixed_groups = []
            for match in matches:
                # 移除外层括号
                group_content = match.strip()[1:-1]

                fixed_group, group_warnings = self._fix_single_value_group(
                    group_content, table_schema, ds_type
                )
                fixed_groups.append(f"({fixed_group})")
                warnings.extend(group_warnings)

            return ", ".join(fixed_groups), warnings

        except Exception as e:
            logger.warning(f"修复VALUES数据时出错: {e}")
            return values_content, [f"VALUES数据修复失败: {str(e)}"]

    def _fix_single_value_group(
        self, group: str, table_schema: Dict[str, Any], ds_type: str
    ) -> Tuple[str, List[str]]:
        """修复单个值组。

        Args:
            group: 值组字符串
            table_schema: 表结构信息
            ds_type: 数据源类型

        Returns:
            Tuple[str, List[str]]: (修复后的值组, 警告列表)
        """
        warnings = []

        try:
            # 解析值
            values = self._parse_values(group)
            columns = table_schema["columns"]

            # 确保值的数量与列数量匹配
            if len(values) != len(columns):
                if len(values) < len(columns):
                    # 添加缺失的值
                    while len(values) < len(columns):
                        col = columns[len(values)]
                        default_val = self._get_default_value_for_column(col, ds_type)
                        values.append(default_val)
                        warnings.append(f"为列 {col['name']} 添加默认值: {default_val}")
                else:
                    # 截断多余的值
                    values = values[: len(columns)]
                    warnings.append(f"截断了多余的值，保持与列数量 {len(columns)} 一致")

            # 验证和修复每个值
            fixed_values = []
            for i, value in enumerate(values):
                if i < len(columns):
                    col = columns[i]
                    fixed_value, col_warnings = self._fix_column_value(
                        value, col, ds_type
                    )
                    fixed_values.append(fixed_value)
                    warnings.extend(col_warnings)
                else:
                    fixed_values.append(value)

            return ", ".join(fixed_values), warnings

        except Exception as e:
            logger.warning(f"修复值组时出错: {e}")
            return group, [f"值组修复失败: {str(e)}"]

    def _parse_values(self, values_str: str) -> List[str]:
        """解析值字符串为值列表。

        Args:
            values_str: 值字符串

        Returns:
            List[str]: 值列表
        """
        values = []
        current_value = ""
        in_single_quote = False
        in_double_quote = False
        escape_next = False
        paren_level = 0

        for char in values_str:
            if escape_next:
                current_value += char
                escape_next = False
                continue

            if char == "\\":
                escape_next = True
                current_value += char
                continue

            # 处理引号
            if char == "'" and not in_double_quote:
                in_single_quote = not in_single_quote
                current_value += char
            elif char == '"' and not in_single_quote:
                in_double_quote = not in_double_quote
                current_value += char
            # 处理括号
            elif not in_single_quote and not in_double_quote:
                if char == "(":
                    paren_level += 1
                    current_value += char
                elif char == ")":
                    paren_level -= 1
                    current_value += char
                elif char == "," and paren_level == 0:
                    # 找到值分隔符
                    if current_value.strip():
                        values.append(current_value.strip())
                    current_value = ""
                else:
                    current_value += char
            else:
                current_value += char

        # 添加最后一个值
        if current_value.strip():
            values.append(current_value.strip())

        return values

    def _fix_column_value(
        self, value: str, col: Dict[str, Any], ds_type: str
    ) -> Tuple[str, List[str]]:
        """修复单个列的值。

        Args:
            value: 原始值
            col: 列信息
            ds_type: 数据源类型

        Returns:
            Tuple[str, List[str]]: (修复后的值, 警告列表)
        """
        warnings = []

        try:
            col_name = col["name"]
            col_type = str(col.get("type", "")).upper()
            nullable = col.get("nullable", True)
            max_length = col.get("max_length")
            min_value = col.get("min_value")
            max_value = col.get("max_value")

            # 处理NULL值
            if value.upper() == "NULL":
                if not nullable:
                    # 不允许NULL，提供默认值
                    default_value = self._get_default_value_for_column(col, ds_type)
                    warnings.append(
                        f"列 {col_name} 不允许NULL，使用默认值: {default_value}"
                    )
                    return default_value, warnings
                else:
                    return "NULL", warnings

            # 处理字符串类型
            if any(t in col_type for t in ["CHAR", "TEXT", "STRING"]):
                # 移除引号获取实际字符串内容
                string_value = value.strip("'\"")

                # 检查长度限制
                if max_length and len(string_value) > max_length:
                    truncated_value = string_value[:max_length]
                    warnings.append(
                        f"列 {col_name} 值过长，已截断: '{string_value}' -> '{truncated_value}'"
                    )
                    return f"'{truncated_value}'", warnings

                # 确保字符串被正确引用
                if not value.startswith("'"):
                    return f"'{string_value}'", warnings

            # 处理数值类型
            elif any(
                t in col_type for t in ["INT", "DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"]
            ):
                try:
                    # 移除引号
                    numeric_str = value.strip("'\"")

                    if "INT" in col_type:
                        numeric_value = int(
                            float(numeric_str)
                        )  # 先转float再转int以处理小数
                    else:
                        numeric_value = float(numeric_str)

                    # 检查数值范围
                    if min_value is not None and numeric_value < min_value:
                        warnings.append(
                            f"列 {col_name} 值小于最小值，已调整: {numeric_value} -> {min_value}"
                        )
                        numeric_value = min_value

                    if max_value is not None and numeric_value > max_value:
                        warnings.append(
                            f"列 {col_name} 值大于最大值，已调整: {numeric_value} -> {max_value}"
                        )
                        numeric_value = max_value

                    # 处理精度和小数位数
                    if col.get("scale") is not None and isinstance(
                        numeric_value, float
                    ):
                        scale = col["scale"]
                        numeric_value = round(numeric_value, scale)

                    return str(numeric_value), warnings

                except (ValueError, TypeError):
                    # 无法转换的数值，提供默认值
                    default_value = self._get_default_value_for_column(col, ds_type)
                    warnings.append(
                        f"列 {col_name} 值无法转换为数值，使用默认值: {default_value}"
                    )
                    return default_value, warnings

            # 处理日期时间类型
            elif any(t in col_type for t in ["DATE", "TIME", "TIMESTAMP"]):
                # 确保日期时间值被正确引用
                if not value.startswith("'"):
                    return f"'{value.strip()}'", warnings

            return value, warnings

        except Exception as e:
            logger.warning(f"修复列 {col.get('name', 'unknown')} 的值时出错: {e}")
            return value, [f"列值修复失败: {str(e)}"]

    def _get_default_value_for_column(self, col: Dict[str, Any], ds_type: str) -> str:
        """为列生成默认值。

        Args:
            col: 列信息
            ds_type: 数据源类型

        Returns:
            str: 默认值
        """
        col_type = str(col.get("type", "")).upper()

        # 检查是否有数据库默认值
        if col.get("has_default") and col.get("default") is not None:
            return str(col["default"])

        # 根据类型生成默认值
        if any(t in col_type for t in ["CHAR", "TEXT", "STRING"]):
            return "''"  # 空字符串
        elif "INT" in col_type:
            min_val = col.get("min_value", 0)
            return str(max(min_val, 1))  # 使用最小值或1
        elif any(t in col_type for t in ["DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"]):
            return "0.0"
        elif "DATE" in col_type:
            return "'1970-01-01'"
        elif "TIME" in col_type:
            return "'00:00:00'"
        elif "TIMESTAMP" in col_type or "DATETIME" in col_type:
            return "'1970-01-01 00:00:00'"
        elif "BOOL" in col_type:
            return "FALSE"
        else:
            return "NULL"  # 最后的默认值

    def _format_column_info(self, col: Dict[str, Any]) -> str:
        """格式化列信息为可读字符串。

        Args:
            col: 列信息字典

        Returns:
            str: 格式化的列信息
        """
        col_name = col["name"]
        col_type = str(col.get("type", ""))

        info_parts = [f"{col_name}({col_type})"]

        # 添加约束信息
        if not col.get("nullable", True):
            info_parts.append("NOT NULL")

        if col.get("max_length"):
            info_parts.append(f"MAX_LEN:{col['max_length']}")

        if col.get("auto_increment"):
            info_parts.append("AUTO_INCREMENT")

        if col.get("has_default"):
            default_val = col.get("default", "DEFAULT")
            info_parts.append(f"DEFAULT:{default_val}")

        if col.get("min_value") is not None or col.get("max_value") is not None:
            min_val = col.get("min_value", "N/A")
            max_val = col.get("max_value", "N/A")
            info_parts.append(f"RANGE:[{min_val},{max_val}]")

        return " ".join(info_parts)

    def _format_constraints_info(self, schema: Dict[str, Any]) -> str:
        """格式化约束信息。

        Args:
            schema: 表结构信息

        Returns:
            str: 格式化的约束信息
        """
        constraints = []

        # 主键约束
        if schema.get("primary_keys"):
            constraints.append(f"- 主键: {', '.join(schema['primary_keys'])}")

        # 外键约束
        if schema.get("foreign_keys"):
            for fk in schema["foreign_keys"]:
                fk_info = f"- 外键: {fk.get('constrained_columns', [])} -> {fk.get('referred_table', 'unknown')}.{fk.get('referred_columns', [])}"
                constraints.append(fk_info)

        # 唯一约束
        if schema.get("unique_constraints"):
            for uc in schema["unique_constraints"]:
                uc_info = f"- 唯一约束: {uc.get('column_names', [])}"
                constraints.append(uc_info)

        # 必填字段
        required_fields = []
        for col in schema.get("columns", []):
            if not col.get("nullable", True) and not col.get("auto_increment", False):
                required_fields.append(col["name"])

        if required_fields:
            constraints.append(f"- 必填字段(NOT NULL): {', '.join(required_fields)}")

        # 有长度限制的字段
        length_limited_fields = []
        for col in schema.get("columns", []):
            if col.get("max_length"):
                length_limited_fields.append(f"{col['name']}(最大{col['max_length']})")

        if length_limited_fields:
            constraints.append(f"- 长度限制: {', '.join(length_limited_fields)}")

        return "\n".join(constraints) if constraints else ""

    def _is_values_well_formed(self, values_content: str) -> bool:
        """检查VALUES内容是否格式良好。

        Args:
            values_content: VALUES子句内容

        Returns:
            bool: 如果格式良好返回True，否则返回False
        """
        try:
            import re

            # 去除首尾空白和换行符
            content = values_content.strip()

            # 先移除注释，简化后续检查
            # 移除行末注释（-- 注释）
            content_no_comments = re.sub(r"--[^\n]*", "", content)

            # 检查是否包含平衡的括号
            open_count = content_no_comments.count("(")
            close_count = content_no_comments.count(")")

            if open_count != close_count or open_count == 0:
                return False

            # 检查是否有明显的格式问题
            # 例如：连续的逗号（但允许在引号内）
            if re.search(r",,", content_no_comments):
                return False

            # 使用更简单但有效的模式检查
            # 检查是否符合 (值1, 值2, ...), (值1, 值2, ...) 的基本模式

            # 提取所有的值组
            value_groups = re.findall(r"\([^)]+\)", content_no_comments)
            if not value_groups:
                return False

            # 检查每个值组内部是否合理
            for group in value_groups:
                inner_content = group[1:-1].strip()  # 移除括号
                if not inner_content:
                    return False

                # 检查是否包含至少一个值（可以是字符串、数字等）
                # 允许单引号、双引号或无引号的值
                if not re.search(r"[^,\s]", inner_content):
                    return False

            # 重新组装所有值组，看是否与原内容基本匹配
            reconstructed = ", ".join(value_groups)
            normalized_original = re.sub(r"\s+", " ", content_no_comments.strip())
            normalized_reconstructed = re.sub(r"\s+", " ", reconstructed.strip())

            # 如果重构的内容基本匹配原内容，则认为格式良好
            # 允许一些空白字符的差异
            return normalized_original.replace(
                " ", ""
            ) == normalized_reconstructed.replace(" ", "")

        except Exception as e:
            logger.debug(f"检查VALUES格式时出错: {e}")
            return False

    async def _get_table_schema_batch(
        self, table_names: List[str], ds_name: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
        """批量获取多个表的结构信息，使用并发优化。

        Args:
            table_names: 表名列表
            ds_name: 数据源名称

        Returns:
            Dict[str, Dict[str, Any]]: 表名到表结构的映射
        """
        # 检查缓存
        cache_key = f"{ds_name or 'default'}"
        schemas = {}
        remaining_tables = []

        for table_name in table_names:
            table_cache_key = f"{cache_key}:{table_name}"
            if table_cache_key in self._schema_cache:
                schemas[table_name] = self._schema_cache[table_cache_key]
            else:
                remaining_tables.append(table_name)

        if not remaining_tables:
            return schemas

        # 并发获取剩余表的结构
        async def get_single_schema(table_name: str):
            try:
                schema = await self.get_table_schema(table_name, ds_name)
                # 缓存结果
                table_cache_key = f"{cache_key}:{table_name}"
                self._schema_cache[table_cache_key] = schema
                return table_name, schema
            except Exception as e:
                logger.warning(f"无法获取表 {table_name} 的结构信息: {e}")
                return table_name, None

        # 使用 asyncio.gather 进行并发处理
        tasks = [get_single_schema(table_name) for table_name in remaining_tables]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"获取表结构时出现异常: {result}")
                continue
            table_name, schema = result
            if schema:
                schemas[table_name] = schema

        return schemas

    async def _get_batch_next_ids(
        self, table_schemas: Dict[str, Dict[str, Any]], ds_name: Optional[str] = None
    ) -> Dict[str, int]:
        """批量获取多个表的下一个可用主键值。

        Args:
            table_schemas: 表结构信息映射
            ds_name: 数据源名称

        Returns:
            Dict[str, int]: 表名到下一个可用ID的映射
        """
        ds_type = await self._get_datasource_type(ds_name)
        if ds_type == "hive":  # Hive通常不使用主键
            return {}

        cache_key = f"{ds_name or 'default'}"
        next_ids = {}

        # 收集需要查询的表和主键列
        tables_to_query = []
        for table_name, schema in table_schemas.items():
            if schema.get("primary_keys"):
                primary_key = schema["primary_keys"][0]  # 假设只有一个主键
                table_cache_key = f"{cache_key}:{table_name}:{primary_key}"

                if table_cache_key in self._next_id_cache:
                    next_ids[table_name] = self._next_id_cache[table_cache_key]
                else:
                    tables_to_query.append((table_name, primary_key))

        if not tables_to_query:
            return next_ids

        # 并发查询下一个可用ID
        async def get_single_next_id(table_name: str, primary_key: str):
            try:
                next_id = await self._get_next_available_id(
                    table_name, primary_key, ds_name
                )
                # 缓存结果（短期缓存，避免并发插入时的冲突）
                table_cache_key = f"{cache_key}:{table_name}:{primary_key}"
                self._next_id_cache[table_cache_key] = next_id
                return table_name, next_id
            except Exception as e:
                logger.warning(f"无法获取表 {table_name} 的下一个ID: {e}")
                return table_name, 1000  # 默认值

        tasks = [
            get_single_next_id(table_name, pk) for table_name, pk in tables_to_query
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"获取下一个ID时出现异常: {result}")
                continue
            table_name, next_id = result
            next_ids[table_name] = next_id

        return next_ids

    def _build_simplified_schema_info(
        self,
        table_schemas: Dict[str, Dict[str, Any]],
        next_ids: Dict[str, int],
        ds_type: str,
    ) -> str:
        """构建简化的表结构信息，减少LLM处理的复杂度。

        Args:
            table_schemas: 表结构信息映射
            next_ids: 下一个可用ID映射
            ds_type: 数据源类型

        Returns:
            str: 简化的表结构信息
        """
        schema_parts = []

        for table_name, schema in table_schemas.items():
            table_info = f"表 {table_name}:\n"

            # 简化列信息 - 只包含必要信息
            if schema.get("is_partitioned", False):
                non_partition_columns = [
                    col
                    for col in schema["columns"]
                    if col["name"] not in schema.get("partition_columns", [])
                ]
                columns_info = [
                    f"{col['name']}({col['type']})" for col in non_partition_columns
                ]
                table_info += f"数据列: {columns_info}\n"
                table_info += f"分区列: {schema['partition_columns']}\n"

                if schema.get("existing_partitions"):
                    table_info += f"现有分区示例: {schema['existing_partitions'][:2]}\n"
            else:
                columns_info = [
                    f"{col['name']}({col['type']})" for col in schema["columns"]
                ]
                table_info += f"列: {columns_info}\n"

            # 添加主键信息
            if schema.get("primary_keys"):
                primary_keys = schema["primary_keys"]
                table_info += f"主键: {primary_keys}\n"

                # 添加下一个可用ID
                if table_name in next_ids:
                    table_info += f"建议起始ID: {next_ids[table_name]}\n"

            # 简化的约束信息 - 只包含最重要的
            constraints = []
            for col in schema.get("columns", []):
                if not col.get("nullable", True):
                    constraints.append(f"{col['name']}:NOT NULL")
                if col.get("auto_increment", False):
                    constraints.append(f"{col['name']}:AUTO_INCREMENT")

            if constraints:
                table_info += f"重要约束: {', '.join(constraints)}\n"

            schema_parts.append(table_info)

        return "\n".join(schema_parts)

    async def get_generation_logs(
        self, limit: int = 10, status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """获取数据生成日志，用于错误诊断。

        Args:
            limit: 返回的日志条数限制
            status: 过滤状态 ('success', 'failed', 'pending')

        Returns:
            List[Dict[str, Any]]: 日志记录列表
        """
        try:
            with self.engine.connect() as conn:
                # 构建查询条件
                where_clause = ""
                params = {"limit": limit}

                if status:
                    where_clause = "WHERE execution_status = :status"
                    params["status"] = status

                query = text(
                    f"""
                    SELECT
                        id,
                        sql_text,
                        ds_name,
                        generated_sql,
                        execution_status,
                        error_message,
                        created_at
                    FROM data_generation_logs
                    {where_clause}
                    ORDER BY created_at DESC
                    LIMIT :limit
                """
                )

                result = conn.execute(query, params)
                logs = []

                for row in result:
                    log_entry = {
                        "id": row[0],
                        "sql_text": row[1],
                        "ds_name": row[2],
                        "generated_sql": row[3],
                        "execution_status": row[4],
                        "error_message": row[5],
                        "created_at": str(row[6]),
                    }
                    logs.append(log_entry)

                logger.info(f"获取到 {len(logs)} 条数据生成日志")
                return logs

        except Exception as e:
            logger.error(f"获取数据生成日志失败: {str(e)}")
            return []

    async def get_recent_errors(self, hours: int = 24) -> List[Dict[str, Any]]:
        """获取最近的错误日志，用于快速诊断。

        Args:
            hours: 查看最近多少小时的错误

        Returns:
            List[Dict[str, Any]]: 错误日志列表
        """
        try:
            with self.engine.connect() as conn:
                query = text(
                    """
                    SELECT
                        id,
                        sql_text,
                        ds_name,
                        error_message,
                        created_at
                    FROM data_generation_logs
                    WHERE execution_status = 'failed'
                        AND created_at >= DATE_SUB(NOW(), INTERVAL :hours HOUR)
                    ORDER BY created_at DESC
                    LIMIT 50
                """
                )

                result = conn.execute(query, {"hours": hours})
                errors = []

                for row in result:
                    error_entry = {
                        "id": row[0],
                        "sql_text": (
                            row[1][:100] + "..." if len(row[1]) > 100 else row[1]
                        ),
                        "ds_name": row[2],
                        "error_message": row[3],
                        "created_at": str(row[4]),
                    }
                    errors.append(error_entry)

                logger.info(f"获取到最近{hours}小时内 {len(errors)} 条错误日志")
                return errors

        except Exception as e:
            logger.error(f"获取错误日志失败: {str(e)}")
            return []

    def format_error_report(self, errors: List[Dict[str, Any]]) -> str:
        """格式化错误报告，便于查看。

        Args:
            errors: 错误日志列表

        Returns:
            str: 格式化的错误报告
        """
        if not errors:
            return "✅ 未发现错误记录"

        report_lines = ["📊 **数据生成错误报告**\n"]
        report_lines.append(f"共发现 {len(errors)} 条错误记录:\n")

        for i, error in enumerate(errors, 1):
            report_lines.append(f"## 错误 {i}")
            report_lines.append(f"**时间**: {error['created_at']}")
            report_lines.append(f"**数据源**: {error.get('ds_name', 'default')}")
            report_lines.append(f"**SQL**: `{error['sql_text']}`")
            report_lines.append(f"**错误信息**: {error['error_message']}")
            report_lines.append("")

        report_content = "\n".join(report_lines)
        # 🚀 新增：确保错误报告的markdown格式正确
        return self._fix_markdown_format(report_content)

    def _estimate_affected_rows(self, sql_statement: str) -> int:
        """预估SQL语句影响的行数。

        Args:
            sql_statement: SQL语句

        Returns:
            int: 预估的影响行数
        """
        try:
            sql_upper = sql_statement.upper().strip()

            # 处理INSERT INTO ... VALUES语句
            if "INSERT INTO" in sql_upper and "VALUES" in sql_upper:
                # 提取VALUES部分
                values_start = sql_upper.find("VALUES")
                if values_start != -1:
                    values_part = sql_statement[values_start + 6 :].strip()

                    # 统计VALUES中的行数
                    # 简单方法：统计VALUES后面括号组的数量
                    import re

                    # 匹配形如 (...), (...), ... 的模式
                    pattern = r"\([^)]*\)"
                    matches = re.findall(pattern, values_part)
                    row_count = len(matches)

                    if row_count > 0:
                        logger.debug(f"预估INSERT VALUES语句影响 {row_count} 行")
                        return row_count

            # 处理REPLACE INTO ... VALUES语句
            elif "REPLACE INTO" in sql_upper and "VALUES" in sql_upper:
                values_start = sql_upper.find("VALUES")
                if values_start != -1:
                    values_part = sql_statement[values_start + 6 :].strip()

                    import re

                    pattern = r"\([^)]*\)"
                    matches = re.findall(pattern, values_part)
                    row_count = len(matches)

                    if row_count > 0:
                        logger.debug(f"预估REPLACE VALUES语句影响 {row_count} 行")
                        return row_count

            # 对于其他类型的语句（如INSERT ... SELECT），无法预估，返回默认值
            logger.debug("无法预估SQL语句影响行数，使用默认值1")
            return 1

        except Exception as e:
            logger.debug(f"预估行数时出错: {e}，使用默认值1")
            return 1

    def _extract_table_from_statement(self, sql_statement: str) -> Optional[str]:
        """从SQL语句中提取表名。

        Args:
            sql_statement: SQL语句

        Returns:
            Optional[str]: 提取的表名，如果无法提取则返回None
        """
        try:
            import re

            sql_upper = sql_statement.upper().strip()

            # 匹配INSERT INTO table_name
            insert_match = re.search(r"INSERT\s+INTO\s+([^\s(]+)", sql_upper)
            if insert_match:
                table_name = insert_match.group(1)
                logger.debug(f"从INSERT语句提取表名: {table_name}")
                return table_name.lower()

            # 匹配REPLACE INTO table_name
            replace_match = re.search(r"REPLACE\s+INTO\s+([^\s(]+)", sql_upper)
            if replace_match:
                table_name = replace_match.group(1)
                logger.debug(f"从REPLACE语句提取表名: {table_name}")
                return table_name.lower()

            # 匹配UPDATE table_name
            update_match = re.search(r"UPDATE\s+([^\s]+)", sql_upper)
            if update_match:
                table_name = update_match.group(1)
                logger.debug(f"从UPDATE语句提取表名: {table_name}")
                return table_name.lower()

            # 匹配DELETE FROM table_name
            delete_match = re.search(r"DELETE\s+FROM\s+([^\s]+)", sql_upper)
            if delete_match:
                table_name = delete_match.group(1)
                logger.debug(f"从DELETE语句提取表名: {table_name}")
                return table_name.lower()

            logger.debug("无法从SQL语句中提取表名")
            return None

        except Exception as e:
            logger.debug(f"提取表名时出错: {e}")
            return None

    def _format_execution_result(
        self, execution_result: Dict[str, Any], detailed: bool = True
    ) -> str:
        """格式化执行结果信息。

        Args:
            execution_result: 执行结果字典
            detailed: 是否显示详细信息

        Returns:
            str: 格式化的结果信息
        """
        total_statements = execution_result.get("total_statements", 0)
        successful_statements = execution_result.get("successful_statements", 0)
        total_rows_affected = execution_result.get("total_rows_affected", 0)
        affected_tables = execution_result.get("affected_tables", [])
        statement_details = execution_result.get("statement_details", [])

        # 基础成功信息
        result_lines = ["✅ **数据生成成功完成！**\n"]

        # 核心统计信息
        if total_rows_affected > 0:
            result_lines.append(
                f"📊 **写入统计**: 成功写入 **{total_rows_affected}** 条数据"
            )
        else:
            result_lines.append("📊 **写入完成**: 数据处理完毕")

        # SQL语句执行统计
        if total_statements > 1:
            result_lines.append(
                f"📝 **执行概况**: {successful_statements}/{total_statements} 条SQL语句执行成功"
            )

        # 影响的表信息
        if affected_tables:
            if len(affected_tables) == 1:
                result_lines.append(f"🎯 **目标表**: {affected_tables[0]}")
            else:
                tables_str = ", ".join(affected_tables)
                result_lines.append(f"🎯 **涉及表**: {tables_str}")

        if detailed:
            # 详细的语句执行信息
            if len(statement_details) > 1:
                result_lines.append(f"\n📋 **详细执行信息**:")
                for detail in statement_details:
                    if detail["status"] == "success":
                        result_lines.append(
                            f"  ✓ 表 `{detail.get('affected_table', '未知')}`: {detail['rows_affected']} 行"
                        )

            # 成功提示
            result_lines.append(f"\n🎉 测试数据已成功写入数据库，可以开始使用了！")
        else:
            # 简化版本只显示基本信息
            result_lines.append(f"\n🎉 测试数据已成功写入数据库。")

        return "\n".join(result_lines) + "\n\n"
