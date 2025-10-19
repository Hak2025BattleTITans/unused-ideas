# --- START OF FILE main.py on Replit (Corrected Parsing Logic) ---

import os
import logging
import time
import pandas as pd
from bs4 import BeautifulSoup

# Устанавливаем все необходимое прямо из скрипта
print("--- Устанавливаем Selenium и зависимости. Это может занять минуту... ---")
# Используем -qq для более тихого вывода
os.system("pip install -q selenium pandas beautifulsoup4 lxml")

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_nalog_data(inn: str, driver: webdriver.Chrome) -> dict:
    main_url = "https://egrul.nalog.ru/index.html"
    data = { "ИНН": inn, "Наименование": "Не найдено", "Руководитель": "Не найдено", "Адрес": "Не найдено" }

    try:
        logging.info(f"[{inn}] Загружаю главную страницу egrul.nalog.ru...")
        driver.get(main_url)

        search_box = WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "query")))
        search_box.clear()
        search_box.send_keys(inn)

        find_button = driver.find_element(By.XPATH, "//button[text()='Найти']")
        find_button.click()

        logging.info(f"[{inn}] Ожидаю результаты поиска...")
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "res-row")))
        
        logging.info(f"[{inn}] УСПЕХ! Результаты получены, парсим HTML.")
        
        soup = BeautifulSoup(driver.page_source, 'lxml')
        result_row = soup.find('div', class_='res-row')
        
        if result_row:
            # Ищем наименование в теге <a> - это надежнее
            name_tag = result_row.find('a')
            if name_tag:
                data['Наименование'] = name_tag.get_text(strip=True)

            #  Получаем весь остальной текст как единое целое
            full_info_text = result_row.get_text(" ", strip=True)
            
            # 3. Извлекаем руководителя из этого текста
            keyword = "ГЕНЕРАЛЬНЫЙ ДИРЕКТОР: "
            if keyword in full_info_text:
                director_part = full_info_text.split(keyword, 1)[1]
                data['Руководитель'] = director_part.split(" ОГРН:")[0].strip()

            # 4. Извлекаем адрес
            address_part = full_info_text.split("ОГРН:")[0]
            # Убираем наименование из адреса, если оно там есть
            if data['Наименование'] != "Не найдено":
                 data['Адрес'] = address_part.replace(data['Наименование'], '').strip()
            else:
                 data['Адрес'] = address_part.strip()

            logging.info(f"[{inn}] Данные успешно разобраны.")
        return data

    except TimeoutException:
        logging.warning(f"[{inn}] Компания не найдена (результаты не появились за 20 секунд).")
        return data
    except Exception as e:
        logging.error(f"[{inn}] Произошла ошибка: {e}")
        return data

# --- Основной блок для запуска ---
if __name__ == "__main__":
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = None
    try:
        logging.info("Запускаем WebDriver в среде Replit...")
        driver = webdriver.Chrome(options=options)
        logging.info("WebDriver успешно запущен!")
        
        inn_list = ["7736207543", "7707083893", "7704217370", "1234567890"]
        all_results = []
        for inn in inn_list:
            result = get_nalog_data(inn, driver)
            all_results.append(result)
            time.sleep(1)
            
        df = pd.DataFrame(all_results)
        print("\n" + "="*80)
        print("--- ФИНАЛЬНЫЙ РЕЗУЛЬТАТ (ЗАПУСК В ОБЛАКЕ REPLIT) ---")
        print(df)
        print("="*80 + "\n")

        # Сохраняем результат в файл
        output_filename = "nalog_data.csv"
        df.to_csv(output_filename, index=False, sep=';', encoding='utf-8-sig')
        logging.info(f"Результаты сохранены в файл '{output_filename}'.")
        logging.info("Ищите его в панели файлов слева. Чтобы скачать, нажмите на три точки рядом с именем файла.")

    except Exception as e:
        logging.error(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        
    finally:
        if driver:
            driver.quit()