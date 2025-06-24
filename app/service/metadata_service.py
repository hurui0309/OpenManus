"""元数据服务模块，提供表级过滤条件查询功能。"""

import logging
import re
from typing import Dict, List, Optional, Set

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class TableFilterCondition:
    """表过滤条件数据类。"""

    def __init__(
        self,
        table_name: str,
        field_name: str,
        filter_condition: str,
        create_time: str = None,
        update_time: str = None,
    ):
        self.table_name = table_name
        self.field_name = field_name
        self.filter_condition = filter_condition
        self.create_time = create_time
        self.update_time = update_time

    def __repr__(self):
        return f"TableFilterCondition(table={self.table_name}, field={self.field_name}, condition={self.filter_condition})"


class MetadataService:
    """元数据服务，提供表级过滤条件查询功能。"""

    def __init__(self, engine: Engine):
        """初始化元数据服务。

        Args:
            engine: 数据库连接引擎
        """
        self.engine = engine
        self.metadata_table = "dm_metadata_table_filters"

    def get_table_filter_conditions(
        self, table_names: List[str]
    ) -> Dict[str, List[TableFilterCondition]]:
        """获取指定表的过滤条件。

        Args:
            table_names: 表名列表，格式为 "database.table" 或 "table"

        Returns:
            字典，key为表名，value为该表的过滤条件列表
        """
        if not table_names:
            return {}

        try:
            # 首先尝试精确匹配
            exact_matches = self._get_exact_matches(table_names)

            # 如果精确匹配没有结果，尝试模糊匹配
            if not exact_matches:
                fuzzy_matches = self._get_fuzzy_matches(table_names)
                return fuzzy_matches

            return exact_matches

        except SQLAlchemyError as e:
            logger.error(f"查询表过滤条件失败: {str(e)}")
            return {}
        except Exception as e:
            logger.error(f"获取表过滤条件时发生未知错误: {str(e)}")
            return {}

    def _get_exact_matches(
        self, table_names: List[str]
    ) -> Dict[str, List[TableFilterCondition]]:
        """精确匹配表名。"""
        placeholders = ", ".join([f":table_{i}" for i in range(len(table_names))])
        query = text(
            f"""
            SELECT table_name, field_name, filter_condition, create_time, update_time
            FROM {self.metadata_table}
            WHERE table_name IN ({placeholders})
            ORDER BY table_name, field_name
        """
        )

        params = {f"table_{i}": table_name for i, table_name in enumerate(table_names)}

        with self.engine.connect() as conn:
            result = conn.execute(query, params)
            rows = result.fetchall()

        return self._organize_results(rows)

    def _get_fuzzy_matches(
        self, table_names: List[str]
    ) -> Dict[str, List[TableFilterCondition]]:
        """模糊匹配表名。"""
        # 构建模糊匹配条件
        like_conditions = []
        params = {}

        for i, table_name in enumerate(table_names):
            # 尝试多种匹配模式
            patterns = self._generate_table_patterns(table_name)

            for j, pattern in enumerate(patterns):
                param_name = f"pattern_{i}_{j}"
                like_conditions.append(f"table_name LIKE :{param_name}")
                params[param_name] = pattern

        if not like_conditions:
            return {}

        query = text(
            f"""
            SELECT table_name, field_name, filter_condition, create_time, update_time
            FROM {self.metadata_table}
            WHERE ({' OR '.join(like_conditions)})
            ORDER BY table_name, field_name
        """
        )

        with self.engine.connect() as conn:
            result = conn.execute(query, params)
            rows = result.fetchall()

        matches = self._organize_results(rows)

        if matches:
            logger.info(f"通过模糊匹配找到 {len(matches)} 个表的过滤条件")

        return matches

    def _generate_table_patterns(self, table_name: str) -> List[str]:
        """为表名生成匹配模式。"""
        patterns = []

        # 移除可能的数据库前缀
        base_name = table_name.split(".")[-1]

        # 模式1: 精确匹配
        patterns.append(table_name)

        # 模式2: 匹配任何数据库下的相同表名
        patterns.append(f"%.{base_name}")

        # 模式3: 单复数变换
        if base_name.endswith("s"):
            # 复数转单数 (users -> user)
            singular = base_name[:-1]
            patterns.extend([f"%.{singular}", singular])
        else:
            # 单数转复数 (user -> users)
            plural = base_name + "s"
            patterns.extend([f"%.{plural}", plural])

        # 模式4: 常见表名变换 (user <-> users)
        if base_name in ["user", "users"]:
            patterns.extend(["%.user", "%.users", "user", "users"])

        return list(set(patterns))  # 去重

    def _organize_results(self, rows) -> Dict[str, List[TableFilterCondition]]:
        """组织查询结果。"""
        table_conditions = {}
        for row in rows:
            table_name = row.table_name
            condition = TableFilterCondition(
                table_name=row.table_name,
                field_name=row.field_name,
                filter_condition=row.filter_condition,
                create_time=str(row.create_time) if row.create_time else None,
                update_time=str(row.update_time) if row.update_time else None,
            )

            if table_name not in table_conditions:
                table_conditions[table_name] = []
            table_conditions[table_name].append(condition)

        logger.info(f"成功获取 {len(table_conditions)} 个表的过滤条件")
        return table_conditions

    def extract_table_names_from_sql(self, sql: str) -> Set[str]:
        """从SQL语句中提取表名。

        Args:
            sql: SQL语句

        Returns:
            表名集合
        """
        try:
            # 移除注释和多余空格
            cleaned_sql = self._clean_sql(sql)

            # 使用正则表达式提取表名
            table_names = set()

            # 匹配 FROM 子句中的表名
            from_pattern = r"\bFROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)"
            from_matches = re.findall(from_pattern, cleaned_sql, re.IGNORECASE)
            table_names.update(from_matches)

            # 匹配 JOIN 子句中的表名
            join_pattern = r"\bJOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)"
            join_matches = re.findall(join_pattern, cleaned_sql, re.IGNORECASE)
            table_names.update(join_matches)

            # 匹配 INSERT INTO 中的表名
            insert_pattern = r"\bINSERT\s+INTO\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)"
            insert_matches = re.findall(insert_pattern, cleaned_sql, re.IGNORECASE)
            table_names.update(insert_matches)

            # 匹配 UPDATE 中的表名
            update_pattern = r"\bUPDATE\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)"
            update_matches = re.findall(update_pattern, cleaned_sql, re.IGNORECASE)
            table_names.update(update_matches)

            # 匹配 DELETE FROM 中的表名
            delete_pattern = r"\bDELETE\s+FROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?)"
            delete_matches = re.findall(delete_pattern, cleaned_sql, re.IGNORECASE)
            table_names.update(delete_matches)

            logger.debug(f"从SQL中提取到表名: {table_names}")
            return table_names

        except Exception as e:
            logger.error(f"提取表名时发生错误: {str(e)}")
            return set()

    def _clean_sql(self, sql: str) -> str:
        """清理SQL语句，移除注释和多余空格。

        Args:
            sql: 原始SQL语句

        Returns:
            清理后的SQL语句
        """
        # 移除单行注释 (-- 注释)
        sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)

        # 移除多行注释 (/* 注释 */)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

        # 移除多余的空白字符
        sql = re.sub(r"\s+", " ", sql).strip()

        return sql

    def format_filter_suggestions(
        self, table_conditions: Dict[str, List[TableFilterCondition]]
    ) -> str:
        """格式化过滤条件建议为可读的文本。

        Args:
            table_conditions: 表过滤条件字典

        Returns:
            格式化后的建议文本
        """
        if not table_conditions:
            return ""

        suggestions = []
        suggestions.append("## 📋 建议的表过滤条件")
        suggestions.append("")

        for table_name, conditions in table_conditions.items():
            suggestions.append(f"### 📊 表: `{table_name}`")
            suggestions.append("")

            for condition in conditions:
                suggestions.append(
                    f"- **字段**: `{condition.field_name}` → **建议条件**: `{condition.filter_condition}`"
                )

            suggestions.append("")

        suggestions.append(
            "💡 **提示**: 请检查您的SQL是否包含了这些重要的过滤条件，以确保查询结果的准确性和性能。"
        )
        suggestions.append("")

        return "\n".join(suggestions)

    def check_missing_filters(
        self, sql: str, table_conditions: Dict[str, List[TableFilterCondition]]
    ) -> List[str]:
        """检查SQL中缺少的过滤条件。

        Args:
            sql: SQL语句
            table_conditions: 表过滤条件字典

        Returns:
            缺少的过滤条件列表
        """
        missing_filters = []

        for table_name, conditions in table_conditions.items():
            for condition in conditions:
                # 检查SQL中是否包含该字段的过滤条件
                field_pattern = rf"\b{re.escape(condition.field_name)}\b"
                if not re.search(field_pattern, sql, re.IGNORECASE):
                    missing_filters.append(
                        f"表 `{table_name}` 缺少字段 `{condition.field_name}` 的过滤条件: `{condition.filter_condition}`"
                    )

        return missing_filters

    def get_filter_suggestions_for_sql(self, sql: str) -> Dict[str, any]:
        """为给定的SQL语句获取完整的过滤条件建议。

        Args:
            sql: SQL语句

        Returns:
            包含建议信息的字典
        """
        try:
            # 提取表名
            table_names = list(self.extract_table_names_from_sql(sql))

            if not table_names:
                return {
                    "has_suggestions": False,
                    "message": "未检测到表名，无法提供过滤条件建议",
                    "suggestions_text": "",
                    "missing_filters": [],
                }

            # 获取过滤条件
            table_conditions = self.get_table_filter_conditions(table_names)

            if not table_conditions:
                return {
                    "has_suggestions": False,
                    "message": f"未找到表 {', '.join(table_names)} 的过滤条件配置",
                    "suggestions_text": "",
                    "missing_filters": [],
                }

            # 格式化建议
            suggestions_text = self.format_filter_suggestions(table_conditions)

            # 检查缺少的过滤条件
            missing_filters = self.check_missing_filters(sql, table_conditions)

            return {
                "has_suggestions": True,
                "table_names": table_names,
                "table_conditions": table_conditions,
                "suggestions_text": suggestions_text,
                "missing_filters": missing_filters,
                "message": f"为 {len(table_conditions)} 个表提供过滤条件建议",
            }

        except Exception as e:
            logger.error(f"获取SQL过滤条件建议失败: {str(e)}")
            return {
                "has_suggestions": False,
                "message": f"获取过滤条件建议时发生错误: {str(e)}",
                "suggestions_text": "",
                "missing_filters": [],
            }
