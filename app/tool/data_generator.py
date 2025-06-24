"""Data generation tool for creating test data based on SQL queries."""

import json
import logging
from datetime import datetime
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
        """获取表结构信息。

        Args:
            table_name: 表名
            ds_name: 数据源名称

        Returns:
            Dict[str, Any]: 表结构信息
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

                # 获取主键信息
                primary_keys = inspector.get_pk_constraint(table_name)

                # 获取外键信息
                foreign_keys = inspector.get_foreign_keys(table_name)

                return {
                    "table_name": table_name,
                    "columns": columns,
                    "primary_keys": primary_keys.get("constrained_columns", []),
                    "foreign_keys": foreign_keys,
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

    def _parse_generated_sql(self, response: str) -> str:
        """解析LLM生成的响应，提取SQL语句。

        Args:
            response (str): LLM生成的响应文本

        Returns:
            str: 返回解析后的SQL语句
        """
        sql_statements = []
        in_sql_block = False
        current_sql = []

        for line in response.split("\n"):
            # We don't strip the line here to preserve indentation within the code block.

            # 处理SQL代码块
            if line.strip().startswith("```sql"):
                in_sql_block = True
                continue
            elif line.strip().startswith("```") and in_sql_block:
                in_sql_block = False
                if current_sql:
                    sql_statements.append("\n".join(current_sql))
                    current_sql = []
                continue

            # 收集SQL语句
            if in_sql_block:
                current_sql.append(line)

        # 处理最后一个SQL块
        if current_sql:
            sql_statements.append("\n".join(current_sql))

        # 合并所有SQL语句，确保每条语句以分号结束
        final_sql = "\n".join(
            stmt if stmt.strip().endswith(";") else f"{stmt.rstrip()};"
            for stmt in sql_statements
        )

        return final_sql

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
    ) -> None:
        """执行数据生成SQL语句。

        Args:
            sql (str): 要执行的SQL语句
            ds_name: 目标数据源名称

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

                        conn.execute(text(stmt))

                    except SQLAlchemyError as e:
                        logger.error(
                            f"Failed to execute SQL statement {i}: {stmt[:100]}..."
                        )
                        logger.error(f"Error: {str(e)}")
                        raise DatabaseError(
                            f"Failed to execute SQL statement {i}: {str(e)}\nSQL: {stmt[:200]}..."
                        )

        except SQLAlchemyError as e:
            logger.error(f"Database operation failed: {str(e)}")
            raise DatabaseError(f"Database operation failed: {str(e)}")

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

            # 获取数据源类型
            ds_type = await self._get_datasource_type(ds_name)

            # 获取表结构信息
            table_schemas = []
            schema_info_parts = []
            for table_name in table_names:
                try:
                    # 使用新的方法获取表信息，包括主键范围
                    table_info = await self._get_table_info_for_llm(table_name, ds_name)
                    schema_info_parts.append(table_info)

                    # 同时保存原始schema用于其他用途
                    schema = await self.get_table_schema(table_name, ds_name)
                    table_schemas.append(schema)
                except DatabaseError:
                    logger.warning(f"无法获取表 {table_name} 的结构信息")

            schema_info = "\n".join(schema_info_parts)

            # 构建用于LLM的提示词
            schema_info_parts = []
            for schema in table_schemas:
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
                        table_info += (
                            f"现有分区示例: {schema['existing_partitions'][:3]}\n"
                        )
                else:
                    table_info += f"列: {[col['name'] + '(' + str(col['type']) + ')' for col in schema['columns']]}\n"

                table_info += f"主键: {schema['primary_keys']}\n"
                schema_info_parts.append(table_info)

            schema_info = "\n".join(schema_info_parts)

            # 根据数据源类型构建特定的提示词
            if ds_type == "mysql":
                # For MySQL, use REPLACE to avoid duplicate key errors
                specific_instructions = """
                **MySQL数据库要求:**
                - **重要:** 请使用 `REPLACE INTO` 而不是 `INSERT INTO` 来避免主键冲突。`REPLACE` 会自动处理重复的主键：如果记录已存在，则删除旧记录，然后插入新记录。
                - 支持批量插入: `REPLACE INTO your_table (column1, column2) VALUES (value1, value2), (value3, value4);`
                """
            elif ds_type == "hive":
                specific_instructions = """
                **Hive数据库要求：**
                1. **分区表INSERT语法**：INSERT INTO TABLE table_name PARTITION(partition_col='value') VALUES (data_columns_only)
                2. **重要**：VALUES中只包含数据列，不包含分区列（分区列在PARTITION子句中指定）
                3. **列数匹配**：VALUES中的列数必须与数据列数量完全一致
                4. 字符串值必须用单引号包围
                5. 支持批量插入：INSERT INTO TABLE table_name PARTITION(ds='2025-06-22') VALUES (row1), (row2), (row3)
                6. **示例**：对于有3个数据列的分区表，正确语法是：
                   INSERT INTO TABLE products PARTITION(ds='2025-06-22') VALUES ('P001', 'Product A', 10.99)
                   错误语法：VALUES ('P001', 'Product A', 10.99', '2025-06-22')  -- 不要在VALUES中包含分区列
                """
            else:  # PostgreSQL and other standard SQL
                specific_instructions = """
                **PostgreSQL/标准SQL数据库要求：**
                1. 使用标准 `INSERT INTO` 语法。
                2. **重要:** 请严格使用 "下一个可用主键值" 提示的起始ID来生成主键，以避免与现有数据发生冲突。
                3. 支持批量插入: `INSERT INTO your_table (column1, column2) VALUES (value1, value2), (value3, value4);`
                """

            prompt = f"""
            你是一个数据生成专家。请基于以下信息生成测试数据的SQL语句。

            原始SQL:
            ```sql
            {sql}
            ```
            数据源: {ds_name or 'default'}
            数据源类型: {ds_type}

            表结构信息:
            {schema_info}

            {specific_instructions}

            请生成包含测试数据的SQL语句。要求：
            1. 严格遵循上面针对特定数据库的指示（例如，为MySQL使用`REPLACE INTO`）。
            2. 生成的数据要能覆盖各种边界情况（例如，空值、最大/最小值、空字符串）。
            3. 如果有外键关系，请确保生成的数据满足外键约束。
            4. 每个表至少生成5-10条有意义的记录。
            5. **返回一个简洁的Markdown格式响应**。响应应包含一个简短的介绍段落，然后是为每个表生成的SQL代码块。**请避免冗长的、重复的解释**，例如关于数据真实性或一致性的说明。
            """

            # 调用LLM生成SQL
            response = await self.llm.ask([{"role": "user", "content": prompt}])

            # 解析生成的SQL
            generated_sql = self._parse_generated_sql(response)

            if not generated_sql:
                error_msg = "LLM未生成有效的SQL语句"
                self._log_generation(sql, ds_name, "", "failed", error_msg)
                return ToolResult(error=error_msg)

            # 执行数据生成SQL
            await self.execute_data_generation(generated_sql, ds_name)

            # 记录成功日志
            self._log_generation(sql, ds_name, generated_sql, "success")

            return ToolResult(output=response)

        except DatabaseError as e:
            error_msg = str(e)
            self._log_generation(
                sql,
                ds_name,
                generated_sql if "generated_sql" in locals() else "",
                "failed",
                error_msg,
            )
            logger.error(f"Data generation failed: {error_msg}")
            return ToolResult(error=f"Data generation failed: {error_msg}")

        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self._log_generation(sql, ds_name, "", "failed", error_msg)
            logger.error(error_msg)
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
