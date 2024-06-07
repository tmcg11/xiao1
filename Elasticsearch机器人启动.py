import time
import logging
import asyncio
from elasticsearch import Elasticsearch
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from botjqr.elasti进行查询 import keys

# 配置日志
logging.basicConfig(filename='bot.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 连接 Elasticsearch
def connect_elasticsearch():
    try:
        es = Elasticsearch(hosts=['http://localhost:9200'], request_timeout=60)
        return es
    except Exception as e:
        logging.error(f"连接 Elasticsearch 失败: {e}")
        return None

es = connect_elasticsearch()

def search_data(query):
    results = []

    if es is None:
        return results

    # 定义要搜索的索引和字段信息
    indices = [
        {
            "index": "中国电话三要素",
            "fields": ["name.keyword", "id_number.keyword", "phone.keyword"],
            "source_names": ["name", "id_number", "phone", "address"],
            "source_labels": ["姓名", "身份证号", "电话", "地址"],
            "source": "电话三要素"
        },
        {
            "index": "xxt",
            "fields": ["name.keyword", "username.keyword", "mobile.keyword", "unique.keyword"],
            "source_names": ["name", "username", "unit", "mobile", "unique"],
            "source_labels": ["编号", "姓名", "单位", "电话", "唯一标识"],
            "source": "学习通"
        },
        {
            "index": "your_index_name",
            "fields": ["name.keyword", "phone.keyword"],
            "source_names": ["name", "phone"],
            "source_labels": ["姓名", "手机号"],
            "source": "姓名手机号"
        },
        {
            "index": "zyuanz",
            "fields": ["name", "cardno", "phone"],  # 将字段名称改为非关键字字段
            "source_names": ["name", "cardno", "phone", "zhengzhi", "nickname"],
            "source_labels": ["姓名", "身份证", "手机号", "政治面貌", "昵称"],
            "source": "志愿者"
        },
        {
            "index": "qq_phone_binding",
            "fields": ["qq_number.keyword", "phone_number.keyword"],
            "source_names": ["qq_number", "phone_number"],
            "source_labels": ["QQ号", "手机号"],
            "source": "QQ绑定"
        },
        {
            "index": "weibo",
            "fields": ["phone.keyword", "uid.keyword"],
            "source_names": ["phone", "uid"],
            "source_labels": ["电话", "UID"],
            "source": "微博"
        }
    ]

    for idx in indices:
        search_body = {
            "query": {
                "simple_query_string": {
                    "query": query,
                    "fields": idx["fields"],
                    "default_operator": "OR"
                }
            }
        }
        search_results = es.search(index=idx["index"], body=search_body)

        # 处理查询结果
        for hit in search_results['hits']['hits']:
            source = hit['_source']
            result_string = f"数据来源: {idx['source']}\n"
            for name, label in zip(idx["source_names"], idx["source_labels"]):
                result_string += f"{label}: {source.get(name, f'未知{label}')}\n"
            results.append(result_string)

    return results



# 定义处理私聊消息的函数
async def private_message_handler(update, context):
    group_link = 't.me/+aroAvu8ceew2MmVl'
    reply_text = f'请在群里面查询信息: {group_link}'
    await update.message.reply_text(reply_text)

# 定义处理 /start 命令的函数
async def start_command(update, context):
    if update.effective_chat.type == 'private':
        await private_message_handler(update, context)
    else:
        await update.message.reply_text('欢迎使用数据查询机器人!请发送 /cx <查询内容> 进行查询。')

# 定义处理 /cx 命令的函数
async def search_command(update, context):
    if update.effective_chat.type == 'private':
        await private_message_handler(update, context)
    else:
        query = ' '.join(context.args)
        if not query:
            await update.message.reply_text('请提供查询内容,例如: /cx QQ号,手机号,姓名,身份证')
            return

        results = search_data(query)
        if results:
            response = '查询结果:\n\n' + '\n\n'.join(results) + '\n\n查询结果来自:t.me/baix112'
        else:
            response = '没有找到匹配的结果。\n\n查询结果来自:t.me/baix112'

        await update.message.reply_text(response)

# 定义处理查看ID命令的函数
async def id_command(update, context):
    if update.effective_chat.type == 'private':
        await private_message_handler(update, context)
    else:
        user_id = str(update.effective_user.id)
        await update.message.reply_text(f"您的用户ID为: {user_id}")

# 定义群组消息处理函数
async def group_message_handler(update, context):
    pass

# 定义错误处理函数
async def error_handler(update, context):
    logging.error(f"发生错误: {context.error}")
    # 添加更详细的错误处理逻辑,如记录日志、通知管理员等

# 定义检测机器人在线状态的函数
async def check_bot_status():
    while True:
        try:
            # 发送一个测试请求给机器人
            await application.bot.get_me()
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 机器人在线")  # 在这里打印机器人在线信息
        except Exception as e:
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 机器人掉线,正在重启...")  # 在这里打印机器人掉线信息
            logging.error(f"{time.strftime('%Y-%m-%d %H:%M:%S')} 机器人掉线,正在重启...: {e}")
            # 重启机器人的逻辑
            restart_bot()

        # 等待60秒再次检测
        await asyncio.sleep(60)

def restart_bot():
    global application
    application = Application.builder().token(keys.token).build()

    # 注册命令处理函数
    application.add_handler(CommandHandler('start', start_command))
    application.add_handler(CommandHandler('cx', search_command))
    application.add_handler(CommandHandler('id', id_command))

    # 注册私聊消息处理函数
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE, private_message_handler))

    # 注册群组消息处理函数
    application.add_handler(MessageHandler(filters.ChatType.GROUPS, group_message_handler))

    # 注册错误处理函数
    application.add_error_handler(error_handler)

    # 启动机器人
    application.run_polling(stop_signals=None)

# 创建 Telegram 机器人应用
application = Application.builder().token(keys.token).build()

# 注册命令处理函数
application.add_handler(CommandHandler('start', start_command))
application.add_handler(CommandHandler('cx', search_command))
application.add_handler(CommandHandler('id', id_command))

# 注册私聊消息处理函数
application.add_handler(MessageHandler(filters.ChatType.PRIVATE, private_message_handler))

# 注册群组消息处理函数
application.add_handler(MessageHandler(filters.ChatType.GROUPS, group_message_handler))

# 注册错误处理函数
application.add_error_handler(error_handler)

# 启动机器人
logging.info('机器人已启动,等待消息...')
application.run_polling()

# 运行检测机器人在线状态的任务
async def main():
    await check_bot_status()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
