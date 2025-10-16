# tests/test_plotting.py
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from investigations.plot_maker import ResearchPlotter

class TestPlotting:
    """Тестирование построения графиков"""
    
    def __init__(self):
        self.plotter = ResearchPlotter()
    
    def test_line_plot(self):
        """Тестирование линейного графика"""
        print("Тестирование линейного графика...")
        try:
            x_data = [10, 20, 50, 100, 200]
            y_data_dict = {
                'Пользователи': [0.1, 0.2, 0.5, 1.0, 2.1],
                'Занятия': [0.3, 0.6, 1.5, 3.0, 6.2]
            }
            
            self.plotter.create_line_plot(
                x_data=x_data,
                y_data_dict=y_data_dict,
                title="Тест: Время генерации данных",
                x_label="Количество строк",
                y_label="Время (секунды)",
                filename="test_line_plot"
            )
            print("✓ Линейный график создан успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка создания линейного графика: {e}")
            return False
    
    def test_bar_chart(self):
        """Тестирование столбчатой диаграммы"""
        print("Тестирование столбчатой диаграммы...")
        try:
            categories = ['SELECT', 'INSERT', 'DELETE']
            values_dict = {
                'Маленькие таблицы': [0.001, 0.002, 0.001],
                'Большие таблицы': [0.01, 0.02, 0.015]
            }
            
            self.plotter.create_bar_chart(
                categories=categories,
                values_dict=values_dict,
                title="Тест: Сравнение времени выполнения запросов",
                x_label="Тип запроса",
                y_label="Время (секунды)",
                filename="test_bar_chart"
            )
            print("✓ Столбчатая диаграмма создана успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка создания столбчатой диаграммы: {e}")
            return False
    
    def test_comparison_plot(self):
        """Тестирование графика сравнения"""
        print("Тестирование графика сравнения...")
        try:
            comparison_data = {
                'Линейная': ([1, 2, 3, 4, 5], [1, 2, 3, 4, 5]),
                'Квадратичная': ([1, 2, 3, 4, 5], [1, 4, 9, 16, 25])
            }
            
            self.plotter.create_comparison_plot(
                data_dict=comparison_data,
                title="Тест: Сравнение зависимостей",
                x_label="X значения",
                y_label="Y значения",
                filename="test_comparison_plot"
            )
            print("✓ График сравнения создан успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка создания графика сравнения: {e}")
            return False

def run_plotting_tests():
    """Запуск всех тестов построения графиков"""
    tester = TestPlotting()
    
    tests = [
        tester.test_line_plot,
        tester.test_bar_chart,
        tester.test_comparison_plot
    ]
    
    print("=== ТЕСТИРОВАНИЕ ПОСТРОЕНИЯ ГРАФИКОВ ===")
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__name__, result))
        except Exception as e:
            print(f"✗ Ошибка в тесте {test.__name__}: {e}")
            results.append((test.__name__, False))
    
    print("\n=== РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ГРАФИКОВ ===")
    for test_name, result in results:
        status = "✓ УСПЕХ" if result else "✗ ОШИБКА"
        print(f"{test_name}: {status}")
    
    return all(result for _, result in results)

if __name__ == "__main__":
    run_plotting_tests()