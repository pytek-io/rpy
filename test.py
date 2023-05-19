import asyncio
import asyncstdlib as a

async def async_squares(i=0):
    """Provide an infinite stream of squared numbers"""
    while True:
        await asyncio.sleep(0.1)
        yield i**2
        i += 1

async def main():
    async_iter = async_squares()
    # loop until we are done
    async for i, s in a.zip(range(5), async_iter):
        print(f"{i}: {s}")
    assert await a.anext(async_iter, "Closed!") == "Closed!"

asyncio.run(main())