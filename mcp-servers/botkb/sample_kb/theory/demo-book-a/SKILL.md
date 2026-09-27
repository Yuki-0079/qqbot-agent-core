---
name: demo-book-a
description: >-
  示例组件 A —— 演示《演示理论手册》的条目结构（chapter / concepts /
  scope_note 等字段）。本目录为 sample_kb 示例数据，不含任何真实作品内容。
---

# 示例组件 A

本目录演示知识库组件的标准结构：

- `references/knowledge-base.json` —— 知识条目（JSON 数组）
- `references/chapters/*.txt` —— 原文章节（可选；首行为标签行）

替换成你自己的书 / 资料库时，保持同样的目录结构即可被 `kb_search.py` 直接加载。
