"""
Модуль с правилами обработки данных для алгоритма подтверждения и актуализации.

Содержит:
1. Иерархию источников данных
2. Правила подтверждения данных
3. Правила актуализации данных
"""

from enum import Enum
from datetime import datetime
from typing import Dict, List, Tuple


class DataSource(Enum):
    """
    Иерархия источников данных в порядке убывания приоритета.
    Чем выше источник в списке, тем выше его приоритет.
    """
    FSIN = "ФСИН"  # Федеральная служба исполнения наказаний
    ROSSTAT = "РОССТАТ"  # Федеральная служба государственной статистики
    AUDIT_RU = "АУДИТ.РУ"  # Публичные аудиторские данные
    MANUAL_CONFIRMED = "РУЧНОЙ ВВОД ПОДТВЕРЖДЕННЫЙ"  # Ручной ввод с подтверждением
    MANUAL_UNCONFIRMED = "РУЧНОЙ ВВОД НЕПОДТВЕРЖДЕННЫЙ"  # Ручной ввод без подтверждения

    @classmethod
    def get_hierarchy_priority(cls, source: 'DataSource') -> int:
        """
        Возвращает числовой приоритет источника по иерархии.
        Меньшее число = более высокий приоритет.
        """
        hierarchy_order = [
            cls.FSIN,
            cls.ROSSTAT,
            cls.AUDIT_RU,
            cls.MANUAL_CONFIRMED,
            cls.MANUAL_UNCONFIRMED
        ]
        return hierarchy_order.index(source)


class ConfirmationStatus(Enum):
    """
    Статусы подтверждения данных в порядке убывания приоритета.
    """
    CONFIRMED = "ПОДТВЕРЖДЕНО"  # Автоматически подтвержденные данные
    MANUAL_CONFIRMED = "РУЧНОЙ ВВОД ПОДТВЕРЖДЕН"  # Ручной ввод с подтверждением
    MANUAL_UNCONFIRMED = "РУЧНОЙ ВВОД НЕ ПОДТВЕРЖДЕН"  # Ручной ввод без подтверждения

    @classmethod
    def get_confirmation_priority(cls, status: 'ConfirmationStatus') -> int:
        """
        Возвращает числовой приоритет статуса подтверждения.
        Меньшее число = более высокий приоритет.
        """
        confirmation_order = [
            cls.CONFIRMED,
            cls.MANUAL_CONFIRMED,
            cls.MANUAL_UNCONFIRMED
        ]
        return confirmation_order.index(status)


class DataRules:
    """
    Класс, содержащий правила обработки данных.
    """

    # Соответствие между источниками данных и их статусами подтверждения
    SOURCE_TO_CONFIRMATION = {
        DataSource.FSIN: ConfirmationStatus.CONFIRMED,
        DataSource.ROSSTAT: ConfirmationStatus.CONFIRMED,
        DataSource.AUDIT_RU: ConfirmationStatus.CONFIRMED,
        DataSource.MANUAL_CONFIRMED: ConfirmationStatus.MANUAL_CONFIRMED,
        DataSource.MANUAL_UNCONFIRMED: ConfirmationStatus.MANUAL_UNCONFIRMED
    }

    @classmethod
    def get_confirmation_status(cls, source: DataSource) -> ConfirmationStatus:
        """
        Возвращает статус подтверждения для указанного источника данных.
        """
        return cls.SOURCE_TO_CONFIRMATION[source]

    @classmethod
    def compare_data_sources(cls, source1: DataSource, source2: DataSource) -> int:
        """
        Сравнивает два источника данных по иерархии.
        Возвращает:
          -1 если source1 имеет более высокий приоритет
           1 если source2 имеет более высокий приоритет
           0 если приоритеты равны
        """
        priority1 = DataSource.get_hierarchy_priority(source1)
        priority2 = DataSource.get_hierarchy_priority(source2)

        if priority1 < priority2:
            return -1
        elif priority1 > priority2:
            return 1
        else:
            return 0

    @classmethod
    def compare_confirmation_status(cls, status1: ConfirmationStatus, status2: ConfirmationStatus) -> int:
        """
        Сравнивает два статуса подтверждения.
        Возвращает:
          -1 если status1 имеет более высокий приоритет
           1 если status2 имеет более высокий приоритет
           0 если приоритеты равны
        """
        priority1 = ConfirmationStatus.get_confirmation_priority(status1)
        priority2 = ConfirmationStatus.get_confirmation_priority(status2)

        if priority1 < priority2:
            return -1
        elif priority1 > priority2:
            return 1
        else:
            return 0


def select_most_relevant_data(data_points: List[Dict]) -> Dict:
    """
    Основная функция алгоритма актуализации данных.

    Правила выбора наиболее актуальных данных:
    1. Сначала по уровню подтверждения данных (более высокий приоритет)
    2. Потом по уровню иерархии источника (более высокий приоритет)
    3. Потом по актуальному времени (более поздняя дата)

    Args:
        data_points: Список словарей с данными. Каждый словарь должен содержать:
                    - 'data': сами данные
                    - 'source': источник данных (DataSource)
                    - 'timestamp': временная метка (datetime)

    Returns:
        Словарь с наиболее актуальными данными.

    Raises:
        ValueError: Если список данных пуст.
    """
    if not data_points:
        raise ValueError("Список данных не может быть пустым")

    # Шаг 1: Группировка по статусу подтверждения
    confirmation_groups = {}
    for point in data_points:
        status = DataRules.get_confirmation_status(point['source'])
        if status not in confirmation_groups:
            confirmation_groups[status] = []
        confirmation_groups[status].append(point)

    # Шаг 2: Выбор группы с наивысшим приоритетом подтверждения
    best_confirmation = min(
        confirmation_groups.keys(),
        key=lambda s: ConfirmationStatus.get_confirmation_priority(s)
    )
    candidates = confirmation_groups[best_confirmation]

    # Шаг 3: Если один кандидат - возвращаем его
    if len(candidates) == 1:
        return candidates[0]

    # Шаг 4: Сортировка по иерархии источников
    source_priority_candidates = sorted(
        candidates,
        key=lambda x: DataSource.get_hierarchy_priority(x['source'])
    )

    # Шаг 5: Выбор кандидатов с наивысшим приоритетом источника
    best_source_priority = DataSource.get_hierarchy_priority(source_priority_candidates[0]['source'])
    best_source_candidates = [
        candidate for candidate in source_priority_candidates
        if DataSource.get_hierarchy_priority(candidate['source']) == best_source_priority
    ]

    # Шаг 6: Если один кандидат - возвращаем его
    if len(best_source_candidates) == 1:
        return best_source_candidates[0]

    # Шаг 7: Сортировка по времени (самые свежие первыми)
    final_candidates = sorted(
        best_source_candidates,
        key=lambda x: x['timestamp'],
        reverse=True
    )

    return final_candidates[0]


'''
if __name__ == "__main__":
    # Демонстрация работы правил
    print("=== ИЕРАРХИЯ ИСТОЧНИКОВ ===")
    for i, source in enumerate(DataSource):
        print(f"{i + 1}. {source.value} (приоритет: {DataSource.get_hierarchy_priority(source)})")

    print("\n=== СТАТУСЫ ПОДТВЕРЖДЕНИЯ ===")
    for i, status in enumerate(ConfirmationStatus):
        print(f"{i + 1}. {status.value} (приоритет: {ConfirmationStatus.get_confirmation_priority(status)})")

    print("\n=== СООТВЕТСТВИЕ ИСТОЧНИКОВ И СТАТУСОВ ===")
    for source in DataSource:
        status = DataRules.get_confirmation_status(source)
        print(f"{source.value} -> {status.value}")

    # Пример данных для тестирования
    test_data = [
        {
            'data': {'value': 100},
            'source': DataSource.MANUAL_UNCONFIRMED,
            'timestamp': datetime(2024, 1, 1, 10, 0)
        },
        {
            'data': {'value': 200},
            'source': DataSource.ROSSTAT,
            'timestamp': datetime(2024, 1, 1, 9, 0)
        },
        {
            'data': {'value': 150},
            'source': DataSource.FSIN,
            'timestamp': datetime(2024, 1, 1, 8, 0)
        }
    ]

    print("\n=== ТЕСТ АЛГОРИТМА ВЫБОРА ===")
    result = select_most_relevant_data(test_data)
    print(f"Выбранные данные: {result['data']}")
    print(f"Источник: {result['source'].value}")
    print(f"Временная метка: {result['timestamp']}")
'''