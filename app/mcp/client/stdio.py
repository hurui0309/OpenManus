"""stdio 客户端实现。"""
import asyncio
from typing import AsyncIterator, Tuple
import sys

from app.mcp import StdioServerParameters

async def stdio_client(params: StdioServerParameters) -> Tuple[AsyncIterator[str], asyncio.StreamWriter]:
    """创建 stdio 客户端连接。

    Args:
        params: stdio 服务器参数

    Returns:
        Tuple[AsyncIterator[str], asyncio.StreamWriter]: 读写流元组
    """
    # 创建子进程
    process = await asyncio.create_subprocess_exec(
        params.command,
        *params.args,
        stdout=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.PIPE,
        stderr=sys.stderr
    )

    if not process.stdout or not process.stdin:
        raise RuntimeError("Failed to create subprocess streams")

    async def read_stream() -> AsyncIterator[str]:
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            yield line.decode('utf-8')

    # 创建写入流
    writer = asyncio.StreamWriter(
        transport=process._transport,
        protocol=process,
        reader=None,
        loop=asyncio.get_event_loop()
    )

    return read_stream(), writer
