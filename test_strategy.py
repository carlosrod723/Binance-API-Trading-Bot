import asyncio
import sanic
from app.api.strategy import setup_strategy_registry

async def test():
    app = sanic.Sanic('test')
    app.ctx = type('obj', (object,), {'config': type('obj', (object,), {})})
    
    try:
        await setup_strategy_registry(app)
        print(f'Successfully loaded {len(app.ctx.strategies)} strategies:')
        for name in app.ctx.strategies.keys():
            print(f"  - {name}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test())
