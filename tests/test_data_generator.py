"""Test cases for DataGeneratorTool.extract_table_names method."""

from unittest.mock import AsyncMock

import pytest

from app.tool.data_generator import DataGeneratorTool


class TestExtractTableNames:
    """Test cases for extract_table_names method."""

    @pytest.fixture
    def tool(self):
        """Fixture for creating a DataGeneratorTool instance."""
        from app.config import Config

        config = Config()  # Initialize with default config
        return DataGeneratorTool(config=config)

    @pytest.mark.asyncio
    async def test_extract_tables_with_llm_success(self, tool):
        """Test extracting tables with LLM successfully."""
        sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        tables = await tool.extract_table_names(sql)

        # 验证返回的表名列表不为空且包含预期表名
        assert len(tables) > 0
        assert all(table in tables for table in ["orders", "users"])

    @pytest.mark.asyncio
    async def test_extract_tables_with_complex_query(self, tool):
        """Test with complex SQL query."""
        sql = """
        SELECT u.name, o.total, p.name
        FROM users u
        JOIN orders o ON u.id = o.user_id
        JOIN order_items oi ON o.id = oi.order_id
        JOIN products p ON oi.product_id = p.id
        WHERE o.status = 'completed'
        """
        tables = await tool.extract_table_names(sql)

        # 验证返回的表名列表包含所有相关表
        assert len(tables) >= 3
        assert all(table in tables for table in ["orders", "users", "products"])

    @pytest.mark.asyncio
    async def test_extract_tables_with_subquery(self, tool):
        """Test with SQL containing subqueries."""
        sql = """
        SELECT u.name, o.total
        FROM users u
        JOIN (
            SELECT user_id, SUM(amount) as total
            FROM ORDERS
            GROUP BY user_id
        ) o ON u.id = o.user_id
        """
        tables = await tool.extract_table_names(sql)
        print(f"=======>{tables}")

        # 验证返回的表名列表
        assert len(tables) >= 2
        assert all(table in tables for table in ["ORDERS", "users"])
