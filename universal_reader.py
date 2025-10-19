import pandas as pd
import json
import csv
import sqlite3
import xml.etree.ElementTree as ET
import yaml
import openpyxl
from pathlib import Path
from typing import Dict, List, Any, Union
from abc import ABC, abstractmethod
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileReader(ABC):
    """Абстрактный базовый класс для чтения файлов"""

    @abstractmethod
    def read(self, file_path: str) -> Union[pd.DataFrame, List[Dict]]:
        pass

    @abstractmethod
    def supported_formats(self) -> List[str]:
        pass


class CSVReader(FileReader):
    """Чтение CSV файлов"""

    def read(self, file_path: str) -> pd.DataFrame:
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Успешно прочитан CSV файл: {file_path}")
            return df
        except Exception as e:
            logger.error(f"Ошибка чтения CSV файла {file_path}: {e}")
            raise

    def supported_formats(self) -> List[str]:
        return ['csv', 'tsv', 'txt']


class ExcelReader(FileReader):
    """Чтение Excel файлов"""

    def read(self, file_path: str) -> Dict[str, pd.DataFrame]:
        try:
            excel_file = pd.ExcelFile(file_path)
            sheets = {}
            for sheet_name in excel_file.sheet_names:
                sheets[sheet_name] = pd.read_excel(file_path, sheet_name=sheet_name)
            logger.info(f"Успешно прочитан Excel файл: {file_path}")
            return sheets
        except Exception as e:
            logger.error(f"Ошибка чтения Excel файла {file_path}: {e}")
            raise

    def supported_formats(self) -> List[str]:
        return ['xlsx', 'xls']


class JSONReader(FileReader):
    """Чтение JSON файлов"""

    def read(self, file_path: str) -> Union[pd.DataFrame, List[Dict]]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Если данные в формате списка словарей - конвертируем в DataFrame
            if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                df = pd.DataFrame(data)
                logger.info(f"Успешно прочитан JSON файл: {file_path}")
                return df
            else:
                logger.info(f"Успешно прочитан JSON файл (сырые данные): {file_path}")
                return data
        except Exception as e:
            logger.error(f"Ошибка чтения JSON файла {file_path}: {e}")
            raise

    def supported_formats(self) -> List[str]:
        return ['json']


class SQLiteReader(FileReader):
    """Чтение SQLite баз данных"""

    def read(self, file_path: str, table_name: str = None) -> Dict[str, pd.DataFrame]:
        try:
            conn = sqlite3.connect(file_path)
            cursor = conn.cursor()

            # Получаем список всех таблиц
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [table[0] for table in cursor.fetchall()]

            data = {}
            for table in tables:
                if table_name is None or table == table_name:
                    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                    data[table] = df

            conn.close()
            logger.info(f"Успешно прочитана SQLite база: {file_path}")
            return data
        except Exception as e:
            logger.error(f"Ошибка чтения SQLite базы {file_path}: {e}")
            raise

    def supported_formats(self) -> List[str]:
        return ['db', 'sqlite', 'sqlite3']


class XMLReader(FileReader):
    """Чтение XML файлов"""

    def read(self, file_path: str, root_element: str = None) -> Union[pd.DataFrame, Dict]:
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            # Простая конвертация XML в список словарей
            data = self._parse_xml_element(root)

            if isinstance(data, list) and data:
                df = pd.DataFrame(data)
                logger.info(f"Успешно прочитан XML файл: {file_path}")
                return df
            else:
                logger.info(f"Успешно прочитан XML файл (сырые данные): {file_path}")
                return data

        except Exception as e:
            logger.error(f"Ошибка чтения XML файла {file_path}: {e}")
            raise

    def _parse_xml_element(self, element):
        """Рекурсивный парсинг XML элемента"""
        result = {}

        # Атрибуты элемента
        if element.attrib:
            result.update(element.attrib)

        # Дочерние элементы
        for child in element:
            child_data = self._parse_xml_element(child)

            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data

        # Текст элемента, если есть и нет дочерних элементов
        if not result and element.text and element.text.strip():
            return element.text.strip()
        elif element.text and element.text.strip() and not any(isinstance(v, dict) for v in result.values()):
            result['_text'] = element.text.strip()

        return result if result else None

    def supported_formats(self) -> List[str]:
        return ['xml']


class FileReaderFactory:
    """Фабрика для создания подходящего ридера"""

    def __init__(self):
        self._readers = {}
        self._register_default_readers()

    def _register_default_readers(self):
        """Регистрация стандартных ридеров"""
        readers = [
            CSVReader(),
            ExcelReader(),
            JSONReader(),
            SQLiteReader(),
            XMLReader()
        ]

        for reader in readers:
            self.register_reader(reader)

    def register_reader(self, reader: FileReader):
        """Регистрация нового ридера"""
        for fmt in reader.supported_formats():
            self._readers[fmt] = reader
        logger.info(f"Зарегистрирован ридер для форматов: {reader.supported_formats()}")

    def get_reader(self, file_path: str) -> FileReader:
        """Получение подходящего ридера для файла"""
        file_extension = Path(file_path).suffix.lower().lstrip('.')

        if file_extension in self._readers:
            return self._readers[file_extension]
        else:
            raise ValueError(f"Неизвестный формат файла: {file_extension}")


class RelationalDataProcessor:
    """Основной класс для обработки реляционных данных"""

    def __init__(self):
        self.factory = FileReaderFactory()
        self.loaded_data = {}

    def read_file(self, file_path: str, **kwargs) -> Any:
        """
        Чтение файла с реляционными данными

        Args:
            file_path: Путь к файлу
            **kwargs: Дополнительные параметры для конкретного ридера

        Returns:
            Данные в формате DataFrame или другом подходящем формате
        """
        if not Path(file_path).exists():
            raise FileNotFoundError(f"Файл не найден: {file_path}")

        reader = self.factory.get_reader(file_path)
        data = reader.read(file_path, **kwargs)

        # Сохраняем загруженные данные
        self.loaded_data[file_path] = data
        return data

    def get_available_formats(self) -> List[str]:
        """Получение списка поддерживаемых форматов"""
        return list(self.factory._readers.keys())

    def add_custom_reader(self, reader: FileReader):
        """Добавление пользовательского ридера"""
        self.factory.register_reader(reader)

    def analyze_data_structure(self, data: Any) -> Dict[str, Any]:
        """Анализ структуры данных"""
        if isinstance(data, pd.DataFrame):
            return {
                'type': 'DataFrame',
                'shape': data.shape,
                'columns': list(data.columns),
                'dtypes': data.dtypes.to_dict(),
                'null_counts': data.isnull().sum().to_dict()
            }
        elif isinstance(data, dict):
            return {
                'type': 'Dictionary',
                'keys': list(data.keys()),
                'size': len(data)
            }
        elif isinstance(data, list):
            return {
                'type': 'List',
                'length': len(data),
                'element_type': type(data[0]).__name__ if data else 'Empty'
            }
        else:
            return {
                'type': type(data).__name__,
                'value': str(data)[:100] + '...' if len(str(data)) > 100 else str(data)
            }


# Пример использования и добавления собственного ридера
class YAMLReader(FileReader):
    """Пример добавления ридера для YAML файлов"""

    def read(self, file_path: str) -> Union[pd.DataFrame, Dict]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)

            # Конвертация в DataFrame если возможно
            if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                df = pd.DataFrame(data)
                logger.info(f"Успешно прочитан YAML файл: {file_path}")
                return df
            else:
                logger.info(f"Успешно прочитан YAML файл (сырые данные): {file_path}")
                return data
        except Exception as e:
            logger.error(f"Ошибка чтения YAML файла {file_path}: {e}")
            raise

    def supported_formats(self) -> List[str]:
        return ['yaml', 'yml']


# Пример использования
'''
def main():
    #processor = RelationalDataProcessor()

    # Добавляем кастомный ридер
       #processor.add_custom_reader(YAMLReader())

    print("Поддерживаемые форматы:", processor.get_available_formats())

    # Пример чтения разных файлов
    try:
        # Чтение CSV
        # csv_data = processor.read_file('data.csv')
        # print("CSV структура:", processor.analyze_data_structure(csv_data))

        # Чтение JSON
        # json_data = processor.read_file('data.json')
        # print("JSON структура:", processor.analyze_data_structure(json_data))

        pass

    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")

'''

'''
class ParquetReader(FileReader):
    #Пример добавления ридера для Parquet
    
    def read(self, file_path: str) -> pd.DataFrame:
        try:
            df = pd.read_parquet(file_path)
            logger.info(f"Успешно прочитан Parquet файл: {file_path}")
            return df
        except Exception as e:
            logger.error(f"Ошибка чтения Parquet файла {file_path}: {e}")
            raise
    
    def supported_formats(self) -> List[str]:
        return ['parquet']

# Регистрация
processor.add_custom_reader(ParquetReader())
'''