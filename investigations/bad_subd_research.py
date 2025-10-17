import time
import timeit
import sys
import os
from typing import List, Dict, Any
import matplotlib.pyplot as plt


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.bad_subd import BadSUBD

class CustomDBResearch:
    def __init__(self):
        self.db = BadSUBD()
        self.results_dir = "investigations/bad_subd"
        os.makedirs(self.results_dir, exist_ok=True)
    
    def setup_test_tables(self):
        """Создание тестовых таблиц для исследования"""
        print("Создание тестовых таблиц...")
        
        # Создаем основную таблицу с индексом
        self.db.engine.create_table("test_indexed", [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR", "size": 50},
            {"name": "value", "type": "INT"}
        ])
        self.db.engine.create_index("test_indexed", "id")
        
        # Создаем таблицу без индекса
        self.db.engine.create_table("test_non_indexed", [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "VARCHAR", "size": 50},
            {"name": "value", "type": "INT"}
        ])
        
        print("Тестовые таблицы созданы")
    
    def generate_test_data(self, num_records: int) -> List[Dict[str, Any]]:
        """Генерация тестовых данных"""
        data = []
        for i in range(num_records):
            data.append({
                "id": i + 1,
                "name": f"user_{i+1}",
                "value": (i * 7) % 100 
            })
        return data
    
    def cleanup_test_data(self):
        """Очистка тестовых данных"""
        print("Очистка тестовых данных...")
        self.db.engine.delete("test_indexed", None)
        self.db.engine.delete("test_non_indexed", None)
    
    def measure_select_with_index(self, target_id: int) -> float:
        """Измерение времени SELECT с индексом"""
        def select_op():
            return self.db.engine.select("test_indexed", where={"id": target_id})
        
        
        time_taken = timeit.timeit(select_op, number=100) / 100
        return time_taken
    
    def measure_select_without_index(self, target_id: int) -> float:
        """Измерение времени SELECT без индекса"""
        def select_op():
            return self.db.engine.select("test_non_indexed", where={"id": target_id})
        
        time_taken = timeit.timeit(select_op, number=100) / 100
        return time_taken
    
    def measure_insert_with_index(self, record: Dict[str, Any]) -> float:
        """Измерение времени INSERT с индексом"""
        def insert_op():
            return self.db.engine.insert("test_indexed", record)
        
        time_taken = timeit.timeit(insert_op, number=10) / 10
        return time_taken
    
    def measure_insert_without_index(self, record: Dict[str, Any]) -> float:
        """Измерение времени INSERT без индекса"""
        def insert_op():
            return self.db.engine.insert("test_non_indexed", record)
        
        time_taken = timeit.timeit(insert_op, number=10) / 10
        return time_taken
    
    def research_select_performance(self):
        """Исследование производительности SELECT"""
        print("=== Исследование производительности SELECT ===")
        
        table_sizes = [250, 500, 750, 1000, 1250, 1500, 1750, 2000]
        results_select = {'С индексом': [], 'Без индекса': []}
        
        for size in table_sizes:
            print(f"Тестирование SELECT для {size} записей...")
            
            # Очищаем и заполняем таблицы
            self.cleanup_test_data()
            test_data = self.generate_test_data(size)
            
            # Вставляем данные в обе таблицы
            for record in test_data:
                self.db.engine.insert("test_indexed", record)
                self.db.engine.insert("test_non_indexed", record)
            
            # Измеряем SELECT для случайного ID
            target_id = size // 2
            
            time_with_index = self.measure_select_with_index(target_id)
            time_without_index = self.measure_select_without_index(target_id)
            
            results_select['С индексом'].append(time_with_index)
            results_select['Без индекса'].append(time_without_index)
            
            print(f"  С индексом: {time_with_index:.6f} сек")
            print(f"  Без индекса: {time_without_index:.6f} сек")
            print(f"  Ускорение: {time_without_index/time_with_index:.2f}x")
        
        # Построение графика
        self._create_comparison_plot(
            table_sizes,
            results_select,
            "Сравнение производительности SELECT",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "select_performance_comparison"
        )
    
    def research_insert_performance(self):
        """Исследование производительности INSERT"""
        print("=== Исследование производительности INSERT ===")
        
        table_sizes = [250, 500, 750, 1000, 1250, 1500, 1750, 2000]
        results_insert = {'С индексом': [], 'Без индекса': []}
        
        for size in table_sizes:
            print(f"Тестирование INSERT для таблицы с {size} существующими записями...")
            
            # Очищаем таблицы
            self.cleanup_test_data()
            
            # Заполняем таблицы начальными данными
            initial_data = self.generate_test_data(size)
            for record in initial_data:
                self.db.engine.insert("test_indexed", record)
                self.db.engine.insert("test_non_indexed", record)
            
            # Генерируем новые данные для вставки
            new_records = self.generate_test_data(10)
            
            # Измеряем время вставки
            total_time_with_index = 0
            total_time_without_index = 0
            
            for record in new_records:
                # Увеличиваем ID чтобы избежать конфликтов
                record_with_index = record.copy()
                record_with_index['id'] = size + record['id']
                
                record_without_index = record.copy()
                record_without_index['id'] = size + record['id'] + 10000
                
                time_with_index = self.measure_insert_with_index(record_with_index)
                time_without_index = self.measure_insert_without_index(record_without_index)
                
                total_time_with_index += time_with_index
                total_time_without_index += time_without_index
            
            avg_time_with_index = total_time_with_index / len(new_records)
            avg_time_without_index = total_time_without_index / len(new_records)
            
            results_insert['С индексом'].append(avg_time_with_index)
            results_insert['Без индекса'].append(avg_time_without_index)
            
            print(f"  С индексом: {avg_time_with_index:.6f} сек")
            print(f"  Без индекса: {avg_time_without_index:.6f} сек")
            print(f"  Замедление: {avg_time_with_index/avg_time_without_index:.2f}x")
        
        # Построение графика
        self._create_comparison_plot(
            table_sizes,
            results_insert,
            "Сравнение производительности INSERT",
            "Количество существующих записей в таблице",
            "Время выполнения (сек)",
            "insert_performance_comparison"
        )
    
    def research_index_scalability(self):
        """Исследование масштабируемости индекса"""
        print("=== Исследование масштабируемости индекса ===")
        
        table_sizes = [250, 500, 750, 1000, 1250, 1500, 1750, 2000]
        results = {'SELECT': [], 'INSERT': []}
        index_sizes = []
        
        for size in table_sizes:
            print(f"Тестирование масштабируемости для {size} записей...")
            
            # Очищаем и заполняем таблицу с индексом
            self.cleanup_test_data()
            test_data = self.generate_test_data(size)
            
            for record in test_data:
                self.db.engine.insert("test_indexed", record)
            
            # Получаем размер индекса
            index_size = self.db.engine.indexes["test_indexed"]["id"].get_index_size()
            index_sizes.append(index_size)
            
            # Измеряем SELECT
            target_id = size // 2
            select_time = self.measure_select_with_index(target_id)
            results['SELECT'].append(select_time)
            
            # Измеряем INSERT
            new_record = {"id": size + 1, "name": f"new_user_{size+1}", "value": 42}
            insert_time = self.measure_insert_with_index(new_record)
            results['INSERT'].append(insert_time)
            
            print(f"  Размер индекса: {index_size} байт")
            print(f"  SELECT: {select_time:.6f} сек")
            print(f"  INSERT: {insert_time:.6f} сек")
        
        # График времени операций
        self._create_scalability_plot(
            table_sizes,
            results,
            "Масштабируемость операций с индексом",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "index_operations_scalability"
        )
        
        # График размера индекса
        self._create_index_size_plot(
            table_sizes,
            index_sizes,
            "Рост размера индекса",
            "Количество записей в таблице",
            "Размер индекса (байт)",
            "index_size_growth"
        )
    
    def _create_comparison_plot(self, x_data, y_data_dict, title, xlabel, ylabel, filename):
        """Создание сравнительного графика"""
        plt.figure(figsize=(10, 6))
        
        colors = ['blue', 'red']
        line_styles = ['-', '--']
        markers = ['o', 's']
        
        for i, (label, y_data) in enumerate(y_data_dict.items()):
            plt.plot(
                x_data, y_data,
                color=colors[i % len(colors)],
                linestyle=line_styles[i % len(line_styles)],
                marker=markers[i % len(markers)] if len(x_data) < 10 else None,
                label=label,
                linewidth=2,
                markersize=6
            )
        
        plt.title(title, fontsize=14, fontweight='bold')
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.legend(fontsize=10)
        plt.tight_layout()
        
        # Сохраняем в разных форматах
        plt.savefig(os.path.join(self.results_dir, f"{filename}.png"), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.results_dir, f"{filename}.pdf"), bbox_inches='tight')
        plt.close()
        
        print(f"График сохранен: {filename}")
    
    def _create_scalability_plot(self, x_data, y_data_dict, title, xlabel, ylabel, filename):
        """Создание графика масштабируемости"""
        plt.figure(figsize=(10, 6))
        
        colors = ['green', 'orange']
        line_styles = ['-', '--']
        markers = ['o', 's']
        
        for i, (label, y_data) in enumerate(y_data_dict.items()):
            plt.plot(
                x_data, y_data,
                color=colors[i % len(colors)],
                linestyle=line_styles[i % len(line_styles)],
                marker=markers[i % len(markers)] if len(x_data) < 10 else None,
                label=label,
                linewidth=2,
                markersize=6
            )
        
        plt.title(title, fontsize=14, fontweight='bold')
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.legend(fontsize=10)
        plt.tight_layout()
        
        plt.savefig(os.path.join(self.results_dir, f"{filename}.png"), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.results_dir, f"{filename}.pdf"), bbox_inches='tight')
        plt.close()
        
        print(f"График сохранен: {filename}")
    
    def _create_index_size_plot(self, x_data, y_data, title, xlabel, ylabel, filename):
        """Создание графика роста размера индекса"""
        plt.figure(figsize=(10, 6))
        
        plt.plot(
            x_data, y_data,
            color='purple',
            linestyle='-',
            marker='o' if len(x_data) < 10 else None,
            linewidth=2,
            markersize=6
        )
        
        plt.title(title, fontsize=14, fontweight='bold')
        plt.xlabel(xlabel, fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        plt.savefig(os.path.join(self.results_dir, f"{filename}.png"), dpi=300, bbox_inches='tight')
        plt.savefig(os.path.join(self.results_dir, f"{filename}.pdf"), bbox_inches='tight')
        plt.close()
        
        print(f"График сохранен: {filename}")
    
    def run_all_research(self):
        """Запуск всех исследований"""
        print("Запуск исследований кастомной СУБД...")
        
        try:
            # Настройка тестовых таблиц
            self.setup_test_tables()
            
            # Запуск исследований
            self.research_select_performance()
            self.research_insert_performance()
            self.research_index_scalability()
            
            print("\nВсе исследования завершены!")
            print(f"Результаты сохранены в папке: {self.results_dir}")
            
        except Exception as e:
            print(f"Ошибка во время исследований: {e}")
            import traceback
            traceback.print_exc()

# Основной скрипт для запуска
if __name__ == "__main__":
    research = CustomDBResearch()
    research.run_all_research()