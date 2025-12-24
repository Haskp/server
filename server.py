import zmq
import json
import psycopg2
from datetime import datetime

counter = 0
DATA_FILE = "data.txt"

# подключение к бд 
DB_CONFIG = {
    'dbname': 'server_data',
    'user': 'hask',
    'password': 'hask123',
    'host': 'localhost',
    'port': '5432'
}

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")
print("ZMQ сервер запущен на порту 5555\nОжидает данные\n")

# соединение с бд 
def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

def print_to_console(data, counter, json_data=None):
    print(f"\n[{counter}] Получены данные:")
    if json_data:
        print(f"    Координаты: {json_data.get('latitude')}, {json_data.get('longitude')}")
        print(f"    Время: {json_data.get('time')}")
        
        gsm = json_data.get('gsm_info')
        if gsm:
            print(f"    Сеть: {gsm.get('network_type')}, сигнал: {gsm.get('signal_strength')} dBm")
            print(f"    MCC: {gsm.get('mcc')}, MNC: {gsm.get('mnc')}")
            print(f"    LAC: {gsm.get('lac')}, CID: {gsm.get('cid')}")
    else:
        print(f"    Текст: {data}")

def save_to_file(data, counter, json_data=None):
    with open(DATA_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{counter}] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Данные: {data}\n")
        
        if json_data:
            f.write(f"Широта: {json_data.get('latitude')}\n")
            f.write(f"Долгота: {json_data.get('longitude')}\n")
            f.write(f"Время: {json_data.get('time')}\n")
            
            gsm = json_data.get('gsm_info')
            if gsm:
                f.write(f"Сеть: {gsm.get('network_type')}\n")
                f.write(f"Сигнал: {gsm.get('signal_strength')} dBm\n")
                f.write(f"MCC: {gsm.get('mcc')}\n")
                f.write(f"MNC: {gsm.get('mnc')}\n")
                f.write(f"LAC: {gsm.get('lac')}\n")
                f.write(f"CID: {gsm.get('cid')}\n")
        
        f.write("-" * 50 + "\n")

def save_to_database(data, counter, json_data=None):
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
            if json_data:
                gsm = json_data.get('gsm_info', {})
                
                cursor.execute("""
                    INSERT INTO unified_data 
                    (counter, latitude, longitude, time, timestamp, mcc, mnc, lac, cid, 
                     network_type, signal_strength, raw_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    counter,
                    json_data.get('latitude'),
                    json_data.get('longitude'),
                    json_data.get('time'),
                    json_data.get('timestamp'),
                    gsm.get('mcc'),
                    gsm.get('mnc'),
                    gsm.get('lac'),
                    gsm.get('cid'),
                    gsm.get('network_type'),
                    gsm.get('signal_strength'),
                    data
                ))
            else:
                cursor.execute("""
                    INSERT INTO unified_data (counter, raw_data)
                    VALUES (%s, %s) RETURNING id
                """, (counter, data))
            
            record_id = cursor.fetchone()[0]
            print(f"Сохранено в БД (ID: {record_id})")
            conn.commit()
            return True
            
    except Exception as e:
        print(f"Ошибка БД: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

while True:
    try:
        message = socket.recv_string()
        
        if message:
            counter += 1
            
            try:
                json_data = json.loads(message)
                print_to_console(message, counter, json_data)
                save_to_file(message, counter, json_data)
                save_to_database(message, counter, json_data)
            except json.JSONDecodeError:
                print_to_console(message, counter)
                save_to_file(message, counter)
                save_to_database(message, counter)
            
            socket.send_string(f"OK #{counter}")
        
    except KeyboardInterrupt:
        print(f"\nВсего записей: {counter}")
        print(f"Данные в файле: {DATA_FILE} и БД: server_data")
        break
    except Exception as e:
        print(f"Ошибка: {e}")

socket.close()
context.term()

#sudo lsof -i :5555
#kiil -9 pid
