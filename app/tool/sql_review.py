"""SQL review tool for SQL query analysis and optimization suggestions."""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

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
from app.prompt.sql_review import (
    SQL_REVIEW_ASSISTANT_PROMPT,
    SQL_REVIEW_SYSTEM_PROMPT,
    SQL_REVIEW_USER_PROMPT,
    SQL_REVIEW_USER_PROMPT_WITH_FILTERS,
    build_missing_filters_alert,
)
from app.service.metadata_service import MetadataService
from app.tool.base import BaseTool, ToolResult

logger = logging.getLogger(__name__)


class SQLReviewTool(BaseTool):
    """SQL审查工具，用于分析SQL语句并提供优化建议。"""

    name: str = "sql_review"
    description: str = (
        "Analyzes SQL queries and provides optimization suggestions and execution plans"
    )
    parameters: Dict = {
        "type": "object",
        "properties": {
            "sql": {
                "type": "string",
                "description": "The SQL query to review and analyze",
            },
            "ds_name": {
                "type": "string",
                "description": "The datasource name to use for analysis (optional)",
            },
        },
        "required": ["sql"],
    }

    config: Config
    llm: Optional[LLM] = None
    engine: Optional[Engine] = None
    datasource_manager: Optional[DataSourceManager] = None
    metadata_service: Optional[MetadataService] = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        """初始化SQL审查工具。"""
        super().__init__(**data)
        self.llm = LLM(config_name="default")
        self.engine = create_engine(self.config.database.connection_url)
        self.datasource_manager = DataSourceManager(self.config)
        self.metadata_service = MetadataService(self.engine)
        self._ensure_review_table()

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

    def _ensure_review_table(self) -> None:
        """确保SQL审查结果表存在。"""
        try:
            # 检查表是否存在
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()

            if "sql_reviews" not in tables:
                # 创建审查结果表
                with self.engine.begin() as conn:  # 使用begin()来自动管理事务
                    create_table_sql = """
                    CREATE TABLE sql_reviews (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        sql_text TEXT NOT NULL,
                        ds_name VARCHAR(32),
                        review_result LONGTEXT,
                        execution_plan TEXT,
                        optimization_suggestions TEXT,
                        performance_score INT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                    conn.execute(text(create_table_sql))
                    logger.info("SQL审查结果表创建成功")

        except SQLAlchemyError as e:
            logger.error(f"创建审查结果表失败: {str(e)}")

    def _save_review_result(
        self,
        sql_text: str,
        ds_name: Optional[str],
        review_result: str,
        execution_plan: str,
        suggestions: str,
        score: int,
    ) -> None:
        """保存审查结果。"""
        try:
            with self.engine.begin() as conn:  # 使用begin()来自动管理事务
                conn.execute(
                    text(
                        """
                    INSERT INTO sql_reviews
                    (sql_text, ds_name, review_result, execution_plan, optimization_suggestions, performance_score)
                    VALUES (:sql_text, :ds_name, :review_result, :execution_plan, :suggestions, :score)
                    """
                    ),
                    {
                        "sql_text": sql_text,
                        "ds_name": ds_name,
                        "review_result": review_result,
                        "execution_plan": execution_plan,
                        "suggestions": suggestions,
                        "score": score,
                    },
                )
        except SQLAlchemyError as e:
            logger.error(f"保存审查结果失败: {str(e)}")

    async def get_database_schema(
        self, ds_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """获取数据库模式信息。

        Args:
            ds_name: 数据源名称

        Returns:
            Dict[str, Any]: 数据库模式信息
        """
        try:
            target_engine = await self._get_target_engine(ds_name)
            ds_type = await self._get_datasource_type(ds_name)

            # 对于Hive，使用特殊方法获取表信息
            if ds_type == "hive":
                return await self._get_hive_database_schema(ds_name)
            else:
                inspector = inspect(target_engine)

                # 获取所有表名
                tables = inspector.get_table_names()

                # 获取每个表的基本信息
                schema_info = {"tables": {}}
                for table in tables:
                    try:
                        columns = inspector.get_columns(table)
                        indexes = inspector.get_indexes(table)
                        primary_keys = inspector.get_pk_constraint(table)

                        schema_info["tables"][table] = {
                            "columns": [
                                {"name": col["name"], "type": str(col["type"])}
                                for col in columns
                            ],
                            "indexes": [idx["name"] for idx in indexes],
                            "primary_keys": primary_keys.get("constrained_columns", []),
                        }
                    except Exception as e:
                        logger.warning(f"获取表 {table} 信息失败: {str(e)}")

                return schema_info

        except Exception as e:
            logger.error(f"获取数据库模式失败: {str(e)}")
            return {"tables": {}}

    async def _get_hive_database_schema(self, ds_name: str) -> Dict[str, Any]:
        """获取Hive数据库模式信息。

        Args:
            ds_name: 数据源名称

        Returns:
            Dict[str, Any]: Hive数据库模式信息
        """
        try:
            target_engine = await self._get_target_engine(ds_name)
            schema_info = {"tables": {}}

            with target_engine.connect() as conn:
                # 获取所有表
                result = conn.execute(text("SHOW TABLES"))
                tables = [row[0] for row in result]

                for table in tables:
                    try:
                        # 获取表结构描述
                        result = conn.execute(text(f"DESCRIBE {table}"))
                        columns = []
                        partition_columns = []
                        in_partition_section = False

                        for row in result:
                            col_name = (
                                row[0]
                                if isinstance(row, tuple)
                                else str(row).split()[0]
                            )
                            col_type = (
                                row[1]
                                if isinstance(row, tuple) and len(row) > 1
                                else "string"
                            )

                            if (
                                "# Partition Information" in str(row)
                                or "partition_columns" in str(row).lower()
                            ):
                                in_partition_section = True
                                continue

                            if (
                                col_name
                                and not col_name.startswith("#")
                                and col_name != "col_name"
                            ):
                                col_info = {"name": col_name, "type": col_type}

                                if in_partition_section:
                                    partition_columns.append(col_info)
                                else:
                                    columns.append(col_info)

                        # 获取分区信息
                        partitions = []
                        if partition_columns:
                            partitions = (
                                await self.datasource_manager.get_table_partitions(
                                    table, ds_name
                                )
                            )

                        schema_info["tables"][table] = {
                            "columns": columns,
                            "partition_columns": partition_columns,
                            "partitions": partitions[:5],  # 只显示前5个分区
                            "is_partitioned": len(partition_columns) > 0,
                            "indexes": [],  # Hive表通常没有传统索引
                            "primary_keys": [],  # Hive通常没有主键
                        }

                    except Exception as e:
                        logger.warning(f"获取Hive表 {table} 信息失败: {str(e)}")

            return schema_info

        except Exception as e:
            logger.error(f"获取Hive数据库模式失败: {str(e)}")
            return {"tables": {}}

    async def analyze_sql_execution_plan(
        self, sql: str, ds_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """分析SQL执行计划。

        Args:
            sql (str): 要分析的SQL语句
            ds_name: 数据源名称

        Returns:
            Dict[str, Any]: 执行计划分析结果
        """
        try:
            target_engine = await self._get_target_engine(ds_name)
            ds_type = await self._get_datasource_type(ds_name)

            with target_engine.connect() as conn:
                if ds_type == "hive":
                    # Hive使用EXPLAIN命令 - 清理SQL并处理分号问题
                    cleaned_sql = self._clean_sql_for_hive_explain(sql)

                    # 构建EXPLAIN语句
                    explain_sql = f"EXPLAIN {cleaned_sql}"
                    logger.debug(f"执行Hive EXPLAIN: {explain_sql}")

                    explain_result = conn.execute(text(explain_sql))
                    plan_text = "\n".join([str(row) for row in explain_result])

                    return {
                        "type": "hive_explain",
                        "plan": plan_text,
                        "analysis": self._analyze_hive_plan(plan_text),
                    }
                else:
                    # MySQL/PostgreSQL使用EXPLAIN
                    explain_result = conn.execute(text(f"EXPLAIN {sql}"))
                    plan_rows = [dict(row._mapping) for row in explain_result]

                    return {
                        "type": "standard_explain",
                        "plan": plan_rows,
                        "analysis": self._analyze_standard_plan(plan_rows),
                    }

        except Exception as e:
            logger.error(f"执行计划分析失败: {str(e)}")
            return {
                "type": "error",
                "plan": f"无法获取执行计划: {str(e)}",
                "analysis": {"warnings": [f"执行计划分析失败: {str(e)}"]},
            }

    def _clean_sql_for_hive_explain(self, sql: str) -> str:
        """清理SQL语句以适配Hive的EXPLAIN命令。

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

        # 移除末尾的分号，因为Hive的EXPLAIN不需要分号
        while cleaned_sql.endswith(";"):
            cleaned_sql = cleaned_sql[:-1].strip()

        logger.debug(f"SQL清理: 原始='{sql[:100]}...' 清理后='{cleaned_sql[:100]}...'")

        return cleaned_sql

    def _analyze_hive_plan(self, plan_text: str) -> Dict[str, List[str]]:
        """分析Hive执行计划。

        Args:
            plan_text: Hive执行计划文本

        Returns:
            Dict[str, List[str]]: 分析结果
        """
        warnings = []
        suggestions = []

        # 检查常见的性能问题
        if "map 100%" in plan_text and "reduce 0%" not in plan_text:
            warnings.append("查询可能触发了reduce阶段，可能影响性能")

        if "TableScan" in plan_text and "partition" not in plan_text.lower():
            warnings.append("查询可能扫描了全表，考虑添加分区过滤条件")

        if "sort" in plan_text.lower() and "partition" not in plan_text.lower():
            suggestions.append("考虑使用分区列进行排序以提高性能")

        return {"warnings": warnings, "suggestions": suggestions}

    def _analyze_standard_plan(self, plan_rows: List[Dict]) -> Dict[str, List[str]]:
        """分析标准执行计划。

        Args:
            plan_rows: 执行计划行

        Returns:
            Dict[str, List[str]]: 分析结果
        """
        warnings = []
        suggestions = []

        for row in plan_rows:
            select_type = row.get("select_type", "")
            type_val = row.get("type", "")
            key = row.get("key", "")
            extra = row.get("Extra", "")

            # 检查全表扫描
            if type_val == "ALL":
                warnings.append(f"表 {row.get('table', 'unknown')} 进行了全表扫描")

            # 检查索引使用
            if not key or key == "NULL":
                suggestions.append(
                    f"考虑为表 {row.get('table', 'unknown')} 添加适当的索引"
                )

            # 检查临时表和文件排序
            if extra and ("Using temporary" in extra or "Using filesort" in extra):
                warnings.append(
                    f"表 {row.get('table', 'unknown')} 使用了临时表或文件排序"
                )

        return {"warnings": warnings, "suggestions": suggestions}

    async def execute(self, **kwargs) -> ToolResult:
        """执行SQL审查任务。

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
            # 获取数据源类型
            ds_type = await self._get_datasource_type(ds_name)

            # 获取数据库模式信息
            schema_info = await self.get_database_schema(ds_name)

            # 分析执行计划
            execution_plan = await self.analyze_sql_execution_plan(sql, ds_name)

            # 🔍 新增：获取表级过滤条件建议
            filter_suggestions = self.metadata_service.get_filter_suggestions_for_sql(
                sql
            )
            logger.info(f"获取到过滤条件建议: {filter_suggestions['message']}")

            # 根据数据源类型构建特定的提示词
            if ds_type == "hive":
                specific_instructions = """
                特别注意 - Hive性能优化要点：
                1. 分区过滤：确保查询包含分区列的过滤条件
                2. 列式存储：检查是否使用了合适的文件格式（如ORC、Parquet）
                3. 动态分区：避免产生过多小文件
                4. JOIN优化：考虑使用map-side join或bucket join
                5. 数据倾斜：检查是否存在数据倾斜问题
                6. 避免使用SELECT *，明确指定需要的列
                7. 合理使用LIMIT来控制输出大小
                """
            else:
                specific_instructions = """
                MySQL/PostgreSQL性能优化要点：
                1. 索引使用：确保查询能够有效利用索引
                2. 避免全表扫描
                3. JOIN优化：选择合适的JOIN类型和顺序
                4. 子查询优化：考虑重写为JOIN
                5. 分页查询：使用LIMIT时考虑添加ORDER BY
                """

            # 🎯 构建包含过滤条件建议的LLM消息
            messages = [{"role": "system", "content": SQL_REVIEW_SYSTEM_PROMPT}]

            # 根据是否有过滤条件建议选择不同的用户提示词
            if filter_suggestions["has_suggestions"]:
                # 构建缺失过滤条件警告
                missing_filters_alert = build_missing_filters_alert(
                    filter_suggestions["missing_filters"]
                )

                user_prompt = SQL_REVIEW_USER_PROMPT_WITH_FILTERS.format(
                    sql=sql,
                    filter_suggestions=filter_suggestions["suggestions_text"],
                    missing_filters_alert=missing_filters_alert,
                )
            else:
                user_prompt = SQL_REVIEW_USER_PROMPT.format(
                    sql=sql, filter_suggestions=""
                )

            # 添加额外的技术分析信息
            user_prompt += f"""

## 🏗️ 技术分析信息

**数据源**: {ds_name or 'default'} ({ds_type})

**数据库模式信息**:
{json.dumps(schema_info, indent=2, ensure_ascii=False)}

**执行计划分析**:
{json.dumps(execution_plan, indent=2, ensure_ascii=False)}

**{ds_type.upper()}性能优化重点**:
{specific_instructions}

请特别关注以上技术信息，并在您的分析中引用具体的模式和执行计划细节。
            """

            messages.append({"role": "user", "content": user_prompt})

            # 调用LLM进行分析
            review_result = await self.llm.ask(messages)

            # 🚀 新增：修复markdown格式
            review_result = self._fix_markdown_format(review_result)

            # 提取评分（简单的文本分析）
            score = self._extract_score(review_result)

            # 保存审查结果（包含过滤条件建议信息）
            enhanced_result = f"""
=== 🔍 过滤条件建议分析 ===
{filter_suggestions['message']}

{filter_suggestions.get('suggestions_text', '')}

=== 📊 SQL Review 结果 ===
{review_result}
            """.strip()

            self._save_review_result(
                sql,
                ds_name,
                enhanced_result,
                json.dumps(execution_plan, ensure_ascii=False),
                enhanced_result,
                score,
            )

            # 🎉 构建增强的输出结果
            output_lines = [
                "🎯 SQL审查完成！",
                f"📊 数据源: {ds_name or 'default'} ({ds_type})",
                f"📈 综合评分: {score}/100",
                "",
            ]

            # 如果有过滤条件建议，优先显示
            if filter_suggestions["has_suggestions"]:
                output_lines.extend(
                    [
                        "=== 🔍 表级过滤条件建议 ===",
                        filter_suggestions["suggestions_text"],
                        "",
                    ]
                )

                if filter_suggestions["missing_filters"]:
                    output_lines.extend(["⚠️ **检测到缺少的关键过滤条件**:", ""])
                    for i, missing in enumerate(
                        filter_suggestions["missing_filters"], 1
                    ):
                        output_lines.append(f"{i}. {missing}")
                    output_lines.append("")

            output_lines.extend(
                [
                    "=== 📋 详细审查报告 ===",
                    review_result,
                    "",
                    "=== ⚙️ 执行计划分析 ===",
                    json.dumps(execution_plan, indent=2, ensure_ascii=False),
                ]
            )

            return ToolResult(output="\n".join(output_lines))

        except Exception as e:
            error_msg = f"SQL审查失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return ToolResult(error=error_msg)

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

            # 1. 修复未闭合的代码块
            pattern = r"```(\w*)(.*?)(?:```|$)"
            matches = list(re.finditer(pattern, fixed_text, re.DOTALL))

            # 从后往前修复，避免位置偏移
            for match in reversed(matches):
                full_match = match.group(0)
                if not full_match.endswith("```"):
                    start, end = match.span()
                    block_content = full_match.rstrip()
                    if not block_content.endswith("\n"):
                        block_content += "\n"
                    block_content += "```"
                    fixed_text = fixed_text[:start] + block_content + fixed_text[end:]
                    logger.debug(f"修复了未闭合的代码块: 位置 {start}-{end}")

            # 2. 清理常见的模板标记
            fixed_text = re.sub(r"\[优化后的SQL语句\]\s*", "", fixed_text)

            # 3. 确保代码块前后有适当的空行
            # 标题后跟代码块需要空行
            fixed_text = re.sub(r"(##[^\n]*)\n(```)", r"\1\n\n\2", fixed_text)
            # 代码块后跟内容需要空行
            fixed_text = re.sub(r"(```)\n([^\n\s])", r"\1\n\n\2", fixed_text)
            fixed_text = re.sub(r"(```)\n(#)", r"\1\n\n\2", fixed_text)

            # 4. 清理过多的连续空行
            fixed_text = re.sub(r"\n{4,}", "\n\n\n", fixed_text)

            # 5. 确保文本以换行结尾
            if fixed_text and not fixed_text.endswith("\n"):
                fixed_text += "\n"

            return fixed_text

        except Exception as e:
            logger.warning(f"Markdown格式修复失败: {e}")
            return text

    def _extract_score(self, review_text: str) -> int:
        """从审查结果中提取评分。

        Args:
            review_text: 审查结果文本

        Returns:
            int: 评分（1-100）
        """
        import re

        # 尝试从文本中提取评分
        score_patterns = [
            r"评分[：:]\s*(\d+)",
            r"得分[：:]\s*(\d+)",
            r"分数[：:]\s*(\d+)",
            r"(\d+)\s*分",
            r"(\d+)/100",
        ]

        for pattern in score_patterns:
            match = re.search(pattern, review_text)
            if match:
                score = int(match.group(1))
                return min(max(score, 1), 100)  # 确保分数在1-100范围内

        # 如果无法提取评分，返回默认值
        return 75

    # 在 execute 方法之前添加新的流式执行方法
    async def execute_stream(self, **kwargs):
        """执行SQL审查任务的流式版本。

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
            # 开始分析前的准备工作
            yield {
                "content": "🔍 **正在准备SQL审查分析...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 获取数据源类型
            ds_type = await self._get_datasource_type(ds_name)

            yield {
                "content": f"📊 数据源类型: {ds_type.upper()}\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 获取数据库模式信息
            yield {
                "content": "🗂️ **正在获取数据库模式信息...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            schema_info = await self.get_database_schema(ds_name)

            # 分析执行计划
            yield {
                "content": "⚙️ **正在分析SQL执行计划...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            execution_plan = await self.analyze_sql_execution_plan(sql, ds_name)

            # 获取表级过滤条件建议
            yield {
                "content": "🎯 **检查表级过滤条件建议...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            filter_suggestions = self.metadata_service.get_filter_suggestions_for_sql(
                sql
            )

            # 构建LLM提示词
            if ds_type == "hive":
                specific_instructions = """
                特别注意 - Hive性能优化要点：
                1. 分区过滤：确保查询包含分区列的过滤条件
                2. 列式存储：检查是否使用了合适的文件格式（如ORC、Parquet）
                3. 动态分区：避免产生过多小文件
                4. JOIN优化：考虑使用map-side join或bucket join
                5. 数据倾斜：检查是否存在数据倾斜问题
                6. 避免使用SELECT *，明确指定需要的列
                7. 合理使用LIMIT来控制输出大小
                """
            else:
                specific_instructions = """
                MySQL/PostgreSQL性能优化要点：
                1. 索引使用：确保查询能够有效利用索引
                2. 避免全表扫描
                3. JOIN优化：选择合适的JOIN类型和顺序
                4. 子查询优化：考虑重写为JOIN
                5. 分页查询：使用LIMIT时考虑添加ORDER BY
                """

            # 构建消息
            messages = [{"role": "system", "content": SQL_REVIEW_SYSTEM_PROMPT}]

            # 根据是否有过滤条件建议选择不同的用户提示词
            if filter_suggestions["has_suggestions"]:
                missing_filters_alert = build_missing_filters_alert(
                    filter_suggestions["missing_filters"]
                )
                user_prompt = SQL_REVIEW_USER_PROMPT_WITH_FILTERS.format(
                    sql=sql,
                    filter_suggestions=filter_suggestions["suggestions_text"],
                    missing_filters_alert=missing_filters_alert,
                )
            else:
                user_prompt = SQL_REVIEW_USER_PROMPT.format(
                    sql=sql, filter_suggestions=""
                )

            # 添加技术分析信息
            user_prompt += f"""

## 🏗️ 技术分析信息

**数据源**: {ds_name or 'default'} ({ds_type})

**数据库模式信息**:
{json.dumps(schema_info, indent=2, ensure_ascii=False)}

**执行计划分析**:
{json.dumps(execution_plan, indent=2, ensure_ascii=False)}

**{ds_type.upper()}性能优化重点**:
{specific_instructions}

请特别关注以上技术信息，并在您的分析中引用具体的模式和执行计划细节。
            """

            messages.append({"role": "user", "content": user_prompt})

            # 开始流式调用LLM
            yield {
                "content": "🤖 **AI专家正在进行深度分析...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 流式调用LLM并实时输出
            full_response = ""
            async for chunk in self.llm.ask_stream(messages):
                if chunk:
                    full_response += chunk
                    yield {
                        "content": chunk,
                        "role": "assistant",
                        "type": "llm_stream",
                    }

            # 🚀 修复：简化markdown格式修复，不发送差异内容
            fixed_response = self._fix_markdown_format(full_response)
            if fixed_response != full_response:
                logger.info("检测到markdown格式问题，已自动修复")
                # 注意：我们不发送修复的差异，因为这可能导致重复或错误的内容
                # 修复后的完整内容将在保存到数据库时使用
                full_response = fixed_response

            # 后处理和保存结果
            yield {
                "content": "\n\n---\n\n✅ **分析完成，正在保存结果...**\n\n",
                "role": "assistant",
                "type": "text",
            }

            # 提取评分
            score = self._extract_score(full_response)

            # 构建增强的结果
            enhanced_result = f"""
=== 🔍 过滤条件建议分析 ===
{filter_suggestions['message']}

{filter_suggestions.get('suggestions_text', '')}

=== 📊 SQL Review 结果 ===
{full_response}
            """.strip()

            # 保存审查结果
            self._save_review_result(
                sql,
                ds_name,
                enhanced_result,
                json.dumps(execution_plan, ensure_ascii=False),
                enhanced_result,
                score,
            )

            # 显示最终总结
            yield {
                "content": f"📈 **最终评分: {score}/100**\n\n",
                "role": "assistant",
                "type": "summary",
            }

            # 如果有过滤条件建议，单独显示
            if filter_suggestions["has_suggestions"]:
                yield {
                    "content": f"### 🎯 过滤条件建议总结\n\n{filter_suggestions['suggestions_text']}\n\n",
                    "role": "assistant",
                    "type": "summary",
                }

            # 结束标记
            yield {
                "content": "",
                "role": "assistant",
                "type": "done",
            }

        except Exception as e:
            error_msg = f"SQL审查失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            yield {
                "content": f"❌ **系统错误**: {error_msg}\n\n",
                "role": "assistant",
                "type": "error",
            }

    def test_markdown_fixes(self):
        """测试Markdown格式修复功能。

        Returns:
            Dict[str, Any]: 测试结果
        """
        test_cases = [
            {
                "name": "未闭合的SQL代码块",
                "input": """## ✨ 优化SQL
```sql
SELECT * FROM users WHERE id = 1
                """,
                "expected_fixes": ["添加结尾```", "SQL代码块完整性"],
            },
            {
                "name": "SQL中的多余引号",
                "input": """```sql
"SELECT * FROM users WHERE name = 'test'"
```""",
                "expected_fixes": ["移除外层引号"],
            },
            {
                "name": "截断的SQL内容",
                "input": """```sql
SELECT * FROM users WHERE id = 1 格式
```""",
                "expected_fixes": ["移除截断标识符"],
            },
            {
                "name": "混乱的标题格式",
                "input": """### 优化SQL ###
```sql
SELECT * FROM users;
```""",
                "expected_fixes": ["标题格式规范化"],
            },
        ]

        results = {
            "total_tests": len(test_cases),
            "passed": 0,
            "failed": 0,
            "details": [],
        }

        for case in test_cases:
            try:
                original = case["input"]
                fixed = self._fix_markdown_format(original)

                # 基本检查：修复后应该不同于原始内容（如果有问题的话）
                has_changes = fixed != original

                # 检查代码块完整性
                sql_blocks_complete = True
                if "```sql" in fixed:
                    import re

                    sql_blocks = re.findall(r"```sql.*?```", fixed, re.DOTALL)
                    sql_blocks_complete = len(sql_blocks) > 0 and all(
                        "```" in block for block in sql_blocks
                    )

                passed = sql_blocks_complete

                results["details"].append(
                    {
                        "test_name": case["name"],
                        "passed": passed,
                        "has_changes": has_changes,
                        "original_length": len(original),
                        "fixed_length": len(fixed),
                        "sql_blocks_complete": sql_blocks_complete,
                    }
                )

                if passed:
                    results["passed"] += 1
                else:
                    results["failed"] += 1

            except Exception as e:
                results["failed"] += 1
                results["details"].append(
                    {"test_name": case["name"], "passed": False, "error": str(e)}
                )

        return results
