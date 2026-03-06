import uvloop
import asyncio
import utils.cythonic_utils # импорт обязателен для контекста
from utils.scanner import ConfigManager, Bot, Main
from aiogram.utils.token import TokenValidationError

if __name__ == '__main__':
    try:
        conf = asyncio.run(ConfigManager.read_config())
        
        bot = None
        tg_id = None 
        try:
            bot_token = conf['telegram']['bot_token']
            bot = Bot(token=bot_token)
            tg_id = conf['telegram']['chat_id']
        except TokenValidationError:
            pass
        
        app = Main(exc_id='bybit', bot=bot)

        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            runner.run(app.run_tasks())

    except KeyboardInterrupt:
        runner.close()