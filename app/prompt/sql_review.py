"""SQL Review prompt templates."""

SQL_REVIEW_SYSTEM_PROMPT = """你是一位专业的SQL Review专家，擅长分析和优化SQL语句。
在进行SQL Review时，你需要重点关注以下方面：

1. 性能优化：
   - 索引使用是否合理
   - 是否存在全表扫描
   - 是否有不必要的子查询
   - 是否可以使用更高效的JOIN策略

2. 代码规范：
   - SQL关键字大写
   - 适当的缩进和换行
   - 有意义的别名
   - 清晰的注释

3. 安全性：
   - 是否存在SQL注入风险
   - 是否有敏感数据泄露风险
   - 权限控制是否合理

4. 可维护性：
   - 代码是否清晰易读
   - 是否有重复逻辑
   - 是否遵循最佳实践

5. 数据准确性：
   - 是否缺少重要的过滤条件
   - 表级别的数据过滤建议
   - 数据范围和边界检查

请针对每个方面提供详细的分析和具体的改进建议。
"""

SQL_REVIEW_USER_PROMPT = """请对以下SQL语句进行全面的Review：

{sql}

{filter_suggestions}

请提供详细的分析报告，包括：
1. 总体评价
2. 具体问题列表（按严重程度排序）
3. 改进建议
4. 过滤条件建议分析（如果有）
5. 优化后的SQL语句（如果需要）
"""

SQL_REVIEW_USER_PROMPT_WITH_FILTERS = """请对以下SQL语句进行全面的Review：

{sql}

## 🔍 表级过滤条件建议
根据数据仓库的元数据分析，发现以下表级过滤条件建议：

{filter_suggestions}

{missing_filters_alert}

请在Review时特别关注：
1. 检查SQL是否包含了上述建议的过滤条件
2. 评估缺少这些过滤条件对查询性能和数据准确性的影响
3. 在优化建议中明确指出需要添加的过滤条件

请提供详细的分析报告，包括：
1. 总体评价
2. 具体问题列表（按严重程度排序）
3. 改进建议
4. 过滤条件建议分析
5. 优化后的SQL语句（如果需要）
"""

SQL_REVIEW_ASSISTANT_PROMPT = """我会按照以下格式提供Review结果：

## 📊 总体评价
[对SQL的整体质量评价]

## ⚠️ 问题列表
### 🔴 严重问题
[影响性能或数据准确性的严重问题]

### 🟡 重要问题
[需要优先解决的重要问题]

### 🔵 普通问题
[一般性问题和建议]

## 💡 改进建议
[具体的改进建议和最佳实践推荐]

## 🔍 过滤条件分析
[基于元数据的过滤条件建议分析]

## ✨ 优化SQL
```sql
[优化后的SQL语句]
```

## 📝 补充说明
[其他需要说明的内容]
"""


def build_missing_filters_alert(missing_filters: list) -> str:
    """构建缺少过滤条件的警告信息。

    Args:
        missing_filters: 缺少的过滤条件列表

    Returns:
        格式化的警告信息
    """
    if not missing_filters:
        return ""

    alert_lines = [
        "## ⚠️ 重要提醒：缺少关键过滤条件",
        "检测到SQL中缺少以下关键过滤条件：",
        "",
    ]

    for i, filter_msg in enumerate(missing_filters, 1):
        alert_lines.append(f"{i}. ❗ {filter_msg}")

    alert_lines.extend(
        [
            "",
            "🚨 **风险提示**: 缺少这些过滤条件可能导致：",
            "- 查询返回过多数据，影响性能",
            "- 数据结果不准确，业务逻辑错误",
            "- 资源消耗过大，影响系统稳定性",
            "",
        ]
    )

    return "\n".join(alert_lines)
