"""Data generation tool for creating test data based on SQL queries."""
import json
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

from sqlalchemy import create_engine, text, inspect, Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config import Config
from app.exceptions import DatabaseError
from app.llm import LLM
from app.prompt.data_generator import (
    DATA_GENERATOR_SYSTEM_PROMPT,
    DATA_GENERATOR_USER_PROMPT,
    DATA_GENERATOR_ASSISTANT_PROMPT
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
                "description": "The SQL query to analyze for data generation"
            }
        },
        "required": ["sql"]
    }

    config: Config
    llm: Optional[LLM] = None
    engine: Optional[Engine] = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        """初始化数据生成工具。"""
        super().__init__(**data)
        self.llm = LLM(config_name="default")
        self.engine = create_engine(self.config.database.connection_url)
        self._ensure_log_table()

    def _ensure_log_table(self):
        """确保数据生成日志表存在。"""
        try:
            with self.engine.connect() as conn:
                # 检查日志表是否存在
                if "mysql" in self.config.database.driver:
                    result = conn.execute(text(
                        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = :db AND table_name = 'data_generation_logs')"
                    ), {"db": self.config.database.database})
                else:
                    result = conn.execute(text(
                        "SELECT EXISTS (SELECT FROM information_schema.tables "
                        "WHERE table_name = 'data_generation_logs')"
                    ))

                if not result.scalar():
                    # 创建日志表
                    create_table_sql = """
                    CREATE TABLE data_generation_logs (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        original_sql TEXT NOT NULL,
                        generated_sql TEXT NOT NULL,
                        affected_tables TEXT NOT NULL,
                        generation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        status VARCHAR(20) NOT NULL,
                        error_message TEXT
                    )
                    """
                    conn.execute(text(create_table_sql))
                    conn.commit()
        except SQLAlchemyError as e:
            logger.error(f"Failed to create log table: {str(e)}")
            raise DatabaseError(f"Failed to create log table: {str(e)}")

    def _log_generation(
        self,
        original_sql: str,
        generated_sql: str,
        affected_tables: List[str],
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """记录数据生成操作到日志表。"""
        try:
            with self.engine.connect() as conn:
                insert_sql = """
                INSERT INTO data_generation_logs (
                    original_sql, generated_sql, affected_tables,
                    status, error_message
                ) VALUES (
                    :original_sql, :generated_sql, :affected_tables,
                    :status, :error_message
                )
                """
                conn.execute(text(insert_sql), {
                    "original_sql": original_sql,
                    "generated_sql": generated_sql,
                    "affected_tables": json.dumps(affected_tables),
                    "status": status,
                    "error_message": error_message
                })
                conn.commit()
        except SQLAlchemyError as e:
            logger.error(f"Failed to log generation: {str(e)}")

    def get_table_schema(self, table_name: str) -> str:
        """获取表结构信息。"""
        try:
            inspector = inspect(self.engine)

            # 获取表的列信息
            columns = inspector.get_columns(table_name)
            column_info = []
            for col in columns:
                nullable = "NULL" if col["nullable"] else "NOT NULL"
                default = f"DEFAULT {col['default']}" if col["default"] is not None else ""
                column_info.append(f"{col['name']} {col['type']} {nullable} {default}".strip())

            # 获取主键信息
            pk = inspector.get_pk_constraint(table_name)
            if pk and pk['constrained_columns']:
                column_info.append(f"PRIMARY KEY ({', '.join(pk['constrained_columns'])})")

            # 获取外键信息
            fks = inspector.get_foreign_keys(table_name)
            for fk in fks:
                referred_cols = fk['referred_columns']
                constrained_cols = fk['constrained_columns']
                referred_table = fk['referred_table']
                column_info.append(
                    f"FOREIGN KEY ({', '.join(constrained_cols)}) "
                    f"REFERENCES {referred_table}({', '.join(referred_cols)})"
                )

            return "\n".join(column_info)

        except SQLAlchemyError as e:
            logger.error(f"Failed to get table schema: {str(e)}")
            raise DatabaseError(f"Failed to get table schema: {str(e)}")

    def extract_table_names(self, sql: str) -> List[str]:
        """从SQL语句中提取表名。"""
        try:
            # 使用SQLAlchemy的text对象解析SQL
            stmt = text(sql)
            # 获取所有引用的表
            tables = []
            for parent in stmt._propagate_attrs["plugin_subject"].froms:
                if hasattr(parent, "name"):
                    tables.append(parent.name)
            return tables
        except Exception as e:
            logger.warning(f"Failed to extract table names from SQL: {str(e)}")
            # 如果解析失败，尝试简单的正则匹配
            import re
            # 匹配 FROM 和 JOIN 后面的表名
            matches = re.findall(r'\b(?:FROM|JOIN)\s+(\w+)', sql.upper())
            return list(set(matches))

    def _parse_generated_sql(self, response: str) -> Tuple[str, str]:
        """解析LLM生成的响应，提取SQL语句和说明。

        Args:
            response (str): LLM生成的响应文本

        Returns:
            Tuple[str, str]: 返回解析后的SQL语句和说明
        """
        sql_statements = []
        description = []
        in_sql_block = False
        current_sql = []

        for line in response.split('\n'):
            line = line.strip()

            # 跳过空行
            if not line:
                continue

            # 处理SQL代码块
            if line.startswith('```sql'):
                in_sql_block = True
                continue
            elif line.startswith('```') and in_sql_block:
                in_sql_block = False
                if current_sql:
                    sql_statements.append('\n'.join(current_sql))
                    current_sql = []
                continue

            # 收集SQL语句
            if in_sql_block:
                current_sql.append(line)
            # 收集说明文本
            elif not line.startswith('#'):
                description.append(line)

        # 处理最后一个SQL块
        if current_sql:
            sql_statements.append('\n'.join(current_sql))

        # 合并所有SQL语句，确保每条语句以分号结束
        final_sql = '\n'.join(
            stmt if stmt.strip().endswith(';') else f"{stmt};"
            for stmt in sql_statements
        )

        return final_sql, '\n'.join(description)

    def execute_data_generation(self, sql: str) -> None:
        """执行数据生成SQL语句。

        Args:
            sql (str): 要执行的SQL语句

        Raises:
            DatabaseError: 当SQL执行失败时抛出
        """
        if not sql.strip():
            raise DatabaseError("Empty SQL statement")

        try:
            with self.engine.connect() as conn:
                # 分割多个SQL语句，保持语句的完整性
                statements = []
                current_stmt = []

                for line in sql.split('\n'):
                    line = line.strip()
                    if not line:
                        continue

                    current_stmt.append(line)
                    if line.endswith(';'):
                        statements.append(' '.join(current_stmt))
                        current_stmt = []

                # 处理最后一个语句
                if current_stmt:
                    stmt = ' '.join(current_stmt)
                    if not stmt.endswith(';'):
                        stmt += ';'
                    statements.append(stmt)

                # 执行每条SQL语句
                for stmt in statements:
                    try:
                        conn.execute(text(stmt))
                    except SQLAlchemyError as e:
                        logger.error(f"Failed to execute SQL: {stmt}")
                        logger.error(f"Error: {str(e)}")
                        raise DatabaseError(f"Failed to execute SQL: {str(e)}\nSQL: {stmt}")

                conn.commit()

        except SQLAlchemyError as e:
            logger.error(f"Database operation failed: {str(e)}")
            raise DatabaseError(f"Database operation failed: {str(e)}")

    async def execute(self, **kwargs) -> ToolResult:
        """执行数据生成。"""
        sql = kwargs.get("sql")
        if not sql:
            return ToolResult(error="SQL query is required")

        try:
            # 获取相关表名
            tables = self.extract_table_names(sql)
            if not tables:
                return ToolResult(error="No tables found in the SQL query")

            # 获取所有相关表的结构
            table_schemas = {}
            for table in tables:
                try:
                    schema = self.get_table_schema(table)
                    table_schemas[table] = schema
                except Exception as e:
                    logger.warning(f"Failed to get schema for table {table}: {str(e)}")

            # 准备prompt
            table_schema_str = "\n\n".join(
                f"Table: {table}\n{schema}"
                for table, schema in table_schemas.items()
            )

            messages = [
                {"role": "system", "content": DATA_GENERATOR_SYSTEM_PROMPT},
                {"role": "user", "content": DATA_GENERATOR_USER_PROMPT.format(
                    sql=sql,
                    table_schema=table_schema_str
                )},
                {"role": "assistant", "content": DATA_GENERATOR_ASSISTANT_PROMPT}
            ]

            # 调用LLM生成数据方案
            response = await self.llm.ask(
                messages=messages,
                temperature=0.2,
                stream=False
            )

            # 解析生成的SQL和说明
            generated_sql, description = self._parse_generated_sql(response)

            try:
                # 执行数据生成
                self.execute_data_generation(generated_sql)
                # 记录成功的生成操作
                self._log_generation(
                    original_sql=sql,
                    generated_sql=generated_sql,
                    affected_tables=tables,
                    status="SUCCESS"
                )
                return ToolResult(output=f"数据生成成功！\n\n{description}")
            except Exception as e:
                # 记录失败的生成操作
                self._log_generation(
                    original_sql=sql,
                    generated_sql=generated_sql,
                    affected_tables=tables,
                    status="FAILED",
                    error_message=str(e)
                )
                raise

        except Exception as e:
            error_msg = f"Data generation failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return ToolResult(error=error_msg)
