import telethon.errors.rpcerrorlist
from telethon import TelegramClient
import json
import requests
import os
import asyncio
import threading
import socks
import time
import random
import logging
from datetime import datetime
from telethon.tl.types import ChannelParticipantsAdmins , UserStatusRecently ,UserStatusOnline , UserStatusOffline
from telethon.tl.types import ChatParticipantCreator, ChatParticipantAdmin, ChannelParticipantCreator
api_id = 0
api_hash = ''

logging.basicConfig(level=logging.INFO, filename=f"{datetime.today().strftime('%d-%m-%Y')}.log",filemode='a+',format="%(asctime)s %(levelname)s %(message)s")
time_delay = [0,1]
time_limit = 4 * 24 * 60 * 60
def check_proxy(login, password, ip, port):
    ip_no_proxy = requests.get('http://api.ipify.org').text
    proxies = {
        'http': f'http://{login}:{password}@{ip}:{port}' ,
        'https': f'http://{login}:{password}@{ip}:{port}'
    }
    try:
        ip_with_proxy = (requests.get('http://api.ipify.org' , proxies=proxies).text)
    except:
        return False
    return ip_no_proxy != ip_with_proxy
def replace_banned_session(session_name):
    if not os.path.exists('banned_sessions'):
        print("Не обнаружена директория banned_sessions")
        logging.info('banned_sessions directory NOT found')
        os.mkdir('banned_sessions')
        print("Создана директория banned_sessions")
        logging.info('banned_sessions directory created')
    os.replace(f'sessions/{session_name}.json', f'banned_sessions/{session_name}.json')
    print(f'файл {session_name}.json перенесен в banned_sessions')
    logging.info(f'file {session_name}.json moved to banned_sessions')
    if os.path.exists(f'sessions/{session_name}.session'):
        os.replace(f'sessions/{session_name}.session' , f'banned_sessions/{session_name}.session')
        print(f'файл {session_name}.session перенесен в banned_sessions')
        logging.info(f'file {session_name}.sessions moved to banned_sessions')
async def get_valid_sessions():
    sessions = [ ]
    for file in os.listdir('sessions'):
        flag = False
        if file.endswith('.json'):
            if os.path.exists(f'sessions/{file[:-5]}.session'):
                if filters[4]:
                    with open(f'sessions/{file}' , 'r') as conf:
                        data = json.load(conf)
                        try:
                            proxy = (socks.SOCKS5, data['proxy'][1],data['proxy'][2],data['proxy'][4],data['proxy'][5])
                        except KeyError:
                            print(f'В файле {file} нет данных для прокси.')
                            logging.info(f'NO proxy-data in {file}')
                            flag = True
                        if check_proxy(proxy[ 3 ] , proxy[ 4 ] , proxy[ 1 ] , proxy[ 2 ]):
                            client = TelegramClient(f'sessions/{file[:-5]}', api_id = api_id, api_hash= api_hash,proxy=proxy)
                            await client.connect()
                            if await client.get_me():
                                print(f'Валидная сессия {file[:-5]}')
                                logging.info(f'session {file[ :-5 ]} valid')
                                sessions.append(file[:-5])
                            else:
                                print(f'Невалидная сессия {file[:-5]}')
                                logging.info(f'session {file[:-5]} NOT valid')
                                flag = True
                            await client.disconnect()
                        else:
                            print(f'Невалиден прокси для сессии {file[:-5]}')
                            logging.info(f'proxy for {file[:-5]} not valid')
                            flag = True
                else:
                    sessions.append(file[:-5])
            else:
                print(f'Для файла {file} не найден файл .session')
                logging.info(f'NO .session file for {file}')
                flag = True
        if flag:
            replace_banned_session(file[:-5])

    if sessions:
        print("Активные сессии:" , *sessions)
        log_text = 'valid sessions '
        for session in sessions:
            log_text += f'{session} '
        logging.info(log_text)
    else:
        print('Нет валидных сессий')
        logging.info('NO valid sessions')
    return sessions
def get_links_for_parsing():
    with open('for_parsing.txt') as file:
        data = file.readlines()
    for i in range(len(data)):
        data[i] = data[i].strip('\n')
    return data
def go_search(session):
    asyncio.run(search(session))
def write_data(members, chat):
    link = chat
    members = list(map(str, members))
    if '@' in chat:
        chat = chat.replace('@', '')
    if 't.me/' in chat:
        chat = chat.replace('t.me/', '')
    if 'https://' in chat:
        chat = chat.replace('https://', '')
    chat = str(len(members)) + ' ' + chat
    if len(members) > 5:
        with open(f'response/{chat}.txt', 'w') as file:
            file.write("\n".join(members))
    else:
        if not os.path.exists('less5response'):
            os.mkdir('less5response')
        with open(f'less5response/{chat}.txt', 'w') as file:
            file.write("\n".join(members))
    logging.info(f'{chat}.txt file created')
    print(f'{chat}.txt файл создан')
    with open('parsed_chats.txt','a') as file:
        file.write(f'{chat}\n')
    with open('for_parsing.txt', 'r') as file:
        lines = file.readlines()
    with open('for_parsing.txt','w') as file:
        for row in lines:
            if row.strip() != link:
                file.write(row)
    logging.info(f'{chat} added to parsed_chats.txt')
    print(f'{chat} записано в parsed_chats.txt')
async def dump_all_participants(client,chat):
    members = list()
    offset = 0
    try:
        async for user in client.iter_participants(chat):

            if offset %250 == 0:
                print("Программа работает")
            if offset%1000 == 0:
                sleep = random.randint(time_delay[0],time_delay[1])
                time.sleep(sleep)
                print(f'Поток {str(threading.currentThread())} ушёл спать на {sleep} секунд')
                logging.info(f'Поток {str(threading.currentThread())} ушёл спать на {sleep} секунд')
            offset += 1
            if not(filters[2] and user.bot) and not(filters[3] and (isinstance(user.participant, ChatParticipantAdmin) or isinstance(user.participant, ChatParticipantCreator) or isinstance(user.participant, ChannelParticipantsAdmins) or isinstance(user.participant, ChannelParticipantCreator))):
                if filters[ 0 ] or filters[ 1 ]:
                    if (isinstance(user.status,UserStatusOnline) or isinstance(user.status, UserStatusRecently)) and filters[0]:
                        if user.username:
                            members.append('@' + user.username)
                    elif filters[1] and (isinstance(user.status, UserStatusRecently) or isinstance(user.status, UserStatusOnline)):
                        if user.username:
                            members.append('@' + user.username)
                    elif filters[1] and isinstance(user.status,UserStatusOffline) and ((datetime.utcnow()-user.status.was_online.replace(tzinfo = None)).seconds + (datetime.utcnow()-user.status.was_online.replace(tzinfo = None)).days*24*60*60)< time_limit:
                        if user.username:
                            members.append('@' + user.username)
                else:
                    if user.username:
                        members.append('@' + user.username)
    except telethon.errors.rpcerrorlist.ChatAdminRequiredError:
        print(f'Не удалось собрать участников группы {chat.username} из-за настроек группы')
        logging.warning(f'ChatAdminRequiredError in {chat.username}')
    return members
def count_done():
    with open('done.txt', 'r') as file:
        done = file.readline()
    if done.isdigit():
        with open('done.txt','w') as file:
            file.write(str(int(done)+1))
    return int(done) + 1
def get_free_session():
    with open('free_sessions.txt', 'r') as file:
        data = file.readlines()
    session = None
    if data:
        session = data.pop().strip()
    with open('free_sessions.txt', 'w') as file:
        for row in data:
            file.write(row.strip() + '\n')
    return  session
async def search(session):
    global targets_for_parsing
    loop = asyncio.new_event_loop()
    with open(f'sessions/{session}.json' , 'r') as conf:
        data = json.load(conf)
    proxy = (socks.SOCKS5 , data[ 'proxy' ][ 1 ] , data[ 'proxy' ][ 2 ] , data[ 'proxy' ][ 4 ] , data[ 'proxy' ][ 5 ])
    client = TelegramClient(f'sessions/{session}' , api_id=api_id , api_hash=api_hash , proxy=proxy)
    flag = False
    while targets_for_parsing:
        if not flag:
            target = targets_for_parsing.pop()
        else:
            flag = False
        await client.connect()
        if await client.get_me():
            await client.disconnect()
            try:
                async with client:
                    try:
                        chat = await client.get_entity(target)
                        members = await dump_all_participants(client,chat)
                        logging.info(f'succesfully parsed chat {target}')
                        write_data(members,target)
                    except ValueError:
                        print(f"Не найден чат по ссылке {target}")
                        logging.info(f'NO entity for {target}')
                        write_data([ ] , target)
                    except telethon.errors.rpcerrorlist.UsernameInvalidError:
                        print(f"Не найден чат по ссылке {target}")
                        logging.info(f'NO entity for {target}')
                        write_data([], target)
                    done = count_done()
                    print(f'Обработано {str(done)} из {must_done}')
                    logging.info(f'{str(done)}/{must_done} done')
            except:
                print(f'Возникла ошибка при работе с сессией {session}')
                logging.warning(f'ERROR with sessions {session}')
                new_session = get_free_session()
                if new_session:
                    print(f'Замена сессии {session} на сессию {new_session}')
                    logging.warning(f'session {session} replaced by {new_session}')
                    with open(f'sessions/{session}.json' , 'r') as conf:
                        data = json.load(conf)
                    proxy = (socks.SOCKS5 , data[ 'proxy' ][ 1 ] , data[ 'proxy' ][ 2 ] , data[ 'proxy' ][ 4 ] ,
                             data[ 'proxy' ][ 5 ])
                    client = TelegramClient(f'sessions/{session}' , api_id=api_id , api_hash=api_hash , proxy=proxy)
                    sleep = random.randint(time_delay[ 0 ] , time_delay[ 1 ])
                    time.sleep(sleep)
                    print(f'Поток {str(threading.currentThread())} ушёл спать на {sleep} секунд')
                    logging.info(f'{str(threading.currentThread())} - sleep for {sleep} seconds')
                else:
                    print(f'{threading.currentThread()} закончил работу, так как нет доступной сессии')
                    logging.warning(f'{threading.currentThread()} stopped working, NO free session')
                    return False
        else:
            flag = True
            print(f'Возникла ошибка при работе с сессией {session}')
            logging.warning(f'ERROR with sessions {session}')
            new_session = get_free_session()
            print(f'Замена сессии {session} на сессию {new_session}')
            logging.warning(f'session {session} replaced by {new_session}')
            with open(f'sessions/{session}.json' , 'r') as conf:
                data = json.load(conf)
            proxy = (socks.SOCKS5 , data[ 'proxy' ][ 1 ] , data[ 'proxy' ][ 2 ] , data[ 'proxy' ][ 4 ] ,
                     data[ 'proxy' ][ 5 ])
            client = TelegramClient(f'sessions/{session}' , api_id=api_id , api_hash=api_hash , proxy=proxy)
async def start():
    global must_done, time_delay
    if not (os.path.exists('response')):
        logging.info('NOT found directory \'response\'')
        print('Не обнаружена директория \'response\'')
        os.mkdir('response')
        logging.info('Created \'response\'')
        print('Создана директория \'response\'')
    global targets_for_parsing, filters, time_limit
    filters = [False, False, False, False, True] #[online_now, was_recently, no_bots, no_admins, check_sessions]
    statuses = {False: "OFF", True: "ON"}
    while True:
        while True:
            print(f"""Настройка фильтров:
            1-[{statuses[filters[0]]}] Парсить пользователей, которые сейчас онлайн.
            2-[{statuses[filters[1]]}] Парсить пользователей, которые были недавно онлайн (не более {str(time_limit//3600)} часов назад)
            3-[{statuses[filters[2]]}] Исключать из парсинга ботов
            4-[{statuses[filters[3]]}] Исключать из парсинга админов, владельцев чатов
            5-[{statuses[filters[4]]}] Проверить валидность сессий перед запуском парсинга
            6-Включить все фильтры
            7-Изменить параметр для второго фильтра
            8-Изменить минимальное время задержки(сейчас - {time_delay[0]})
            9-Изменить максимальное время задержки(сейчас - {time_delay[1]})
        """)
            switch = input('Введит номер фильтра, который хотите переключить. Для перехода к следующему этапа нажмите Enter.\n')
            if switch:
                if switch in ['1','2','3','4','5']:
                    filters[int(switch)-1] = not(filters[int(switch)-1])
                elif switch == '6':
                    for i in range(4):
                        filters[i] = True
                elif switch == '7':
                    while True:
                        print('Введите новое значение(в часах):')
                        limit = input()
                        if limit.isdigit():
                            time_limit = int(limit)*3600
                            break
                elif switch == '8' or switch == '9':
                    while True:
                        print('Введите новое значение(в секундах):')
                        time_sleep = input()
                        if time_sleep.isdigit() and int(time_sleep) >= 0:
                            if switch == '8' and int(time_sleep) < time_delay[1]:
                                time_delay[0] = int(time_sleep)
                                break
                            elif switch == '9' and int(time_sleep) > time_delay[0]:
                                time_delay[1] = int(time_sleep)
                                break
                else:
                    print('Введено некорректное значение.')
            else:
                break
        logging.info(f'filters_parameters: online_now-{filters[0]}; was_recently-{filters[1]}; no_bots-{filters[2]}; no_admins-{filters[3]} ')
        print('Начинаем проверку сессий и прокси на валидность...')
        sessions = await get_valid_sessions()
        with open('done.txt', 'w') as file:
            file.write('0')
        targets_for_parsing = get_links_for_parsing()
        must_done = len(targets_for_parsing)
        threads = list()
        for i in range(min(len(targets_for_parsing),len(sessions))):
            threads.append(threading.Thread(target=go_search, args= [sessions[i]]))
        if len(targets_for_parsing) < len(sessions):
            free_sessions = sessions[len(targets_for_parsing):]
        else:
            free_sessions = list()
        with open(f'free_sessions.txt', 'w') as file:
            for session in free_sessions:
                file.write(session + '\n')
        for thread in threads:
            thread.start()
        flag = True
        while flag:
            flag = False
            for thread in threads:
                if thread.is_alive():
                    flag = True
        print('Программа успешно закончила работу.')
        logging.info('script succesfully finished.')
        mode = input('Введите exit для окончания, нажмите enter для продолжения работы:')
        if mode == 'exit':
            break
asyncio.run(start())
