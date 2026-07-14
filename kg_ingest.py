import asyncio
import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j_graphrag.llm import OpenAILLM
from neo4j_graphrag.embeddings.openai import OpenAIEmbeddings
from neo4j_graphrag.experimental.components.text_splitters.fixed_size_splitter import FixedSizeSplitter
from neo4j_graphrag.experimental.pipeline.kg_builder import SimpleKGPipeline

# 加载 .env 文件中的环境变量
load_dotenv()

async def main():
    print("1. 正在连接 Neo4j 数据库...")
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))
    )

    print("2. 初始化大模型与向量模型 (自动读取中转站配置)...")
    # 抽取图谱推荐开启 JSON 格式强制输出，温度设为 0 保证稳定性
    llm = OpenAILLM(
        model_name="gpt-4o",
        model_params={
            "temperature": 0, 
            "response_format": {"type": "json_object"}
        }
    )
    # 用于将文本切片转化为高维向量，存入 Neo4j
    embedder = OpenAIEmbeddings(model="text-embedding-3-small")

    print("3. 定义我们要抽取的图谱骨架 (Schema)...")
    # 告诉 LLM，遇到文本时只能提取以下几种节点和关系
    node_labels = ["Person", "Organization", "Location", "Concept"]
    rel_types = ["WORKS_FOR", "LOCATED_IN", "FOUNDED_BY", "RELATED_TO"]

    print("4. 构建并运行 Knowledge Graph Pipeline...")
    kg_builder = SimpleKGPipeline(
        llm=llm,
        driver=driver,
        embedder=embedder,
        text_splitter=FixedSizeSplitter(chunk_size=500, chunk_overlap=100),
        entities=node_labels,
        relations=rel_types,
        from_file=False  # 设置为 False 代表我们直接传入文本字符串，而不是文件路径
    )

    # 准备一段测试文本
    test_text = """
    苹果公司（Apple Inc.）的总部位于美国加利福尼亚州的库比蒂诺。
    蒂姆·库克（Tim Cook）是该公司的现任首席执行官。
    """
    
    print("5. 开始执行抽取与入库（调用大模型中，请稍候）...")
    result = await kg_builder.run_async(text=test_text)
    
    print("\n✅ 入库完成！提取结果摘要：")
    print(result)

if __name__ == "__main__":
    asyncio.run(main())