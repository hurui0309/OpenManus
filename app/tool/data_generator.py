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
            with self.engine.connect() as conn:
                # 检查表是否存在
                inspector = inspect(self.engine)
                tables = inspector.get_table_names()

                if "data_generation_logs" not in tables:
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
                    conn.commit()
                    logger.info("数据生成日志表创建成功")

        except SQLAlchemyError as e:
            logger.error(f"创建日志表失败: {str(e)}")

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
            with self.engine.connect() as conn:
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
                conn.commit()
        except SQLAlchemyError as e:
            logger.error(f"记录数据生成日志失败: {str(e)}")

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
                    col_name = row[0] if isinstance(row, tuple) else str(row).split()[0]
                    col_type = (
                        row[1] if isinstance(row, tuple) and len(row) > 1 else "string"
                    )

                    # 检查是否到达分区信息部分
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
                        col_info = {
                            "name": col_name,
                            "type": col_type,
                            "nullable": True,
                            "default": None,
                        }

                        if in_partition_section:
                            partition_columns.append(col_info)
                        else:
                            columns.append(col_info)

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

            # 使用 begin() 来管理事务
            with target_engine.begin() as conn:
                # 分割多个SQL语句，保持语句的完整性
                statements = []
                current_stmt = []

                for line in sql.split("\n"):
                    line = line.strip()
                    if not line:
                        continue

                    current_stmt.append(line)
                    if line.endswith(";"):
                        statements.append(" ".join(current_stmt))
                        current_stmt = []

                # 处理最后一个语句
                if current_stmt:
                    stmt = " ".join(current_stmt)
                    if not stmt.endswith(";"):
                        stmt += ";"
                    statements.append(stmt)

                # 执行每条SQL语句
                for stmt in statements:
                    try:
                        # 对于Hive，可能需要特殊处理
                        if ds_type == "hive":
                            # Hive可能需要设置一些参数
                            if "INSERT" in stmt.upper():
                                # 设置动态分区参数
                                conn.execute(
                                    text("SET hive.exec.dynamic.partition = true")
                                )
                                conn.execute(
                                    text(
                                        "SET hive.exec.dynamic.partition.mode = nonstrict"
                                    )
                                )

                        conn.execute(text(stmt))
                    except SQLAlchemyError as e:
                        logger.error(f"Failed to execute SQL: {stmt}")
                        logger.error(f"Error: {str(e)}")
                        raise DatabaseError(
                            f"Failed to execute SQL: {str(e)}\nSQL: {stmt}"
                        )

                # 事务会在 with 块结束时自动提交

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
                table_info += f"列: {[col['name'] + '(' + str(col['type']) + ')' for col in schema['columns']]}\n"
                table_info += f"主键: {schema['primary_keys']}\n"

                # 如果是Hive分区表，添加分区信息
                if schema.get("is_partitioned", False):
                    table_info += f"分区表 - 分区列: {schema['partition_columns']}\n"
                    if schema.get("existing_partitions"):
                        table_info += f"现有分区示例: {schema['existing_partitions'][:3]}\n"  # 只显示前3个分区

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
                1. 如果表是分区表，INSERT语句必须包含PARTITION子句
                2. 分区表的INSERT语法：INSERT INTO TABLE table_name PARTITION(partition_col='value') VALUES (...)
                3. 或者使用动态分区：INSERT INTO TABLE table_name PARTITION(partition_col) VALUES (..., partition_value)
                4. 字符串值必须用单引号包围
                5. 支持批量插入：INSERT INTO TABLE table_name VALUES (row1), (row2), (row3)
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
            table_info += f"列: {[col['name'] + '(' + str(col['type']) + ')' for col in schema['columns']]}\n"
            table_info += f"主键: {schema['primary_keys']}\n"

            # 获取主键列的下一个可用ID
            if schema["primary_keys"]:
                primary_key = schema["primary_keys"][0]  # 假设只有一个主键
                next_id = await self._get_next_available_id(
                    table_name, primary_key, ds_name
                )
                table_info += f"下一个可用主键值: {next_id}\n"

            # 如果是Hive分区表，添加分区信息
            if schema.get("is_partitioned", False):
                table_info += f"分区表 - 分区列: {schema['partition_columns']}\n"
                if schema.get("existing_partitions"):
                    table_info += f"现有分区示例: {schema['existing_partitions'][:3]}\n"

            return table_info

        except Exception as e:
            logger.warning(f"获取表 {table_name} 信息失败: {e}")
            return f"表 {table_name}: 无法获取详细信息"
