"""SSE 客户端实现。"""
import asyncio
from typing import AsyncIterator, Tuple
import aiohttp

async def sse_client(url: str) -> Tuple[AsyncIterator[str], asyncio.StreamWriter]:
    """创建 SSE 客户端连接。

    Args:
        url: SSE 服务器 URL

    Returns:
        Tuple[AsyncIterator[str], asyncio.StreamWriter]: 读写流元组
    """
    session = aiohttp.ClientSession()
    async with session.get(url) as response:
        if response.status != 200:
            raise ConnectionError(f"Failed to connect to SSE server: {response.status}")

        async def read_stream() -> AsyncIterator[str]:
            async for line in response.content:
                if line:
                    yield line.decode('utf-8')

        # 创建一个虚拟的写入流
        writer = asyncio.StreamWriter(
            transport=None,
            protocol=None,
            reader=None,
            loop=asyncio.get_event_loop()
        )

        return read_stream(), writer
