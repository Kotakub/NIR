# lib/plotter.py
import matplotlib.pyplot as plt
import matplotlib as mpl
from typing import List, Dict, Any, Optional, Tuple, Union
import os
import numpy as np

class ResearchPlotter:
    """
    Класс для построения исследовательских графиков с помощью matplotlib
    """
    
    def __init__(self, output_dir: str = "investigations/plots"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self._setup_plot_style()
    
    def _setup_plot_style(self):
        """Настройка стиля графиков"""
        plt.style.use('default')
        mpl.rcParams.update({
            'font.family': 'DejaVu Sans',  # Поддержка русского языка
            'font.size': 12,
            'figure.figsize': (10, 6),
            'axes.titlesize': 14,
            'axes.labelsize': 12,
            'legend.fontsize': 10,
            'xtick.labelsize': 10,
            'ytick.labelsize': 10,
            'grid.alpha': 0.3,
            'grid.linestyle': '--'
        })
    
    def create_line_plot(self, 
                        x_data: List[Union[int, float]],
                        y_data_dict: Dict[str, List[Union[int, float]]],
                        title: str,
                        x_label: str, 
                        y_label: str,
                        filename: str,
                        save_formats: List[str] = ['png', 'svg']
                        ):
        """
        Создает линейный график с несколькими рядами данных
        
        Args:
            x_data: данные для оси X (например, размеры данных)
            y_data_dict: словарь {название_ряда: данные_Y}
            title: заголовок графика
            x_label: подпись оси X
            y_label: подпись оси Y
            filename: имя файла для сохранения (без расширения)
            save_formats: форматы для сохранения
        """
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Палитра цветов для рядов
        colors = plt.cm.tab10(np.linspace(0, 1, len(y_data_dict)))
        
        # Стили линий
        line_styles = ['-', '--', '-.', ':']
        
        # Типы маркеров
        markers = ['o', 's', '^', 'D', 'v', '<', '>', 'p', '*', 'X']
        
        for i, (label, y_data) in enumerate(y_data_dict.items()):
            # Определяем стиль для этого ряда
            color = colors[i]
            line_style = line_styles[i % len(line_styles)]
            marker = markers[i % len(markers)] if len(x_data) < 10 else None
            
            # Строим линию
            ax.plot(x_data, y_data,
                   color=color,
                   linestyle=line_style,
                   marker=marker,
                   markersize=8,
                   markerfacecolor=color,
                   markeredgecolor=color,
                   markeredgewidth=1,
                   linewidth=2,
                   label=label)
        
        # Настройка внешнего вида графика
        self._format_plot(ax, title, x_label, y_label)
        
        # Сохранение в указанных форматах
        self._save_plot(fig, filename, save_formats)
        
        plt.close(fig)
    
    def create_comparison_plot(self, 
                             data_dict: Dict[str, Tuple[List, List]],
                             title: str,
                             x_label: str, 
                             y_label: str,
                             filename: str,
                             save_formats: List[str] = ['png', 'svg']) -> None:
        """
        Создает график сравнения нескольких наборов данных
        
        Args:
            data_dict: словарь {метка: (x_data, y_data)}
            title: заголовок графика
            x_label: подпись оси X
            y_label: подпись оси Y
            filename: имя файла для сохранения
            save_formats: форматы для сохранения
        """
        fig, ax = plt.subplots(figsize=(10, 6))
        
        colors = plt.cm.Set2(np.linspace(0, 1, len(data_dict)))
        line_styles = ['-', '--', '-.', ':']
        markers = ['o', 's', '^', 'D', 'v', '<', '>', 'p', '*']
        
        for i, (label, (x_data, y_data)) in enumerate(data_dict.items()):
            color = colors[i]
            line_style = line_styles[i % len(line_styles)]
            marker = markers[i % len(markers)] if len(x_data) < 10 else None
            
            ax.plot(x_data, y_data,
                   color=color,
                   linestyle=line_style,
                   marker=marker,
                   markersize=8,
                   linewidth=2,
                   label=label)
        
        self._format_plot(ax, title, x_label, y_label)
        self._save_plot(fig, filename, save_formats)
        plt.close(fig)
    
    def create_bar_chart(self,
                        categories: List[str],
                        values_dict: Dict[str, List[float]],
                        title: str,
                        x_label: str,
                        y_label: str,
                        filename: str,
                        save_formats: List[str] = ['png', 'svg']) -> None:
        """
        Создает столбчатую диаграмму для сравнения величин
        
        Args:
            categories: категории по оси X
            values_dict: словарь {название_группы: значения}
            title: заголовок
            x_label: подпись оси X
            y_label: подпись оси Y
            filename: имя файла
            save_formats: форматы сохранения
        """
        fig, ax = plt.subplots(figsize=(10, 6))
        
        x = np.arange(len(categories))
        width = 0.8 / len(values_dict)  # Ширина столбцов
        colors = plt.cm.tab10(np.linspace(0, 1, len(values_dict)))
        
        for i, (label, values) in enumerate(values_dict.items()):
            offset = (i - len(values_dict) / 2 + 0.5) * width
            ax.bar(x + offset, values, width, label=label, color=colors[i])
        
        ax.set_xticks(x)
        ax.set_xticklabels(categories, rotation=45, ha='right')
        
        self._format_plot(ax, title, x_label, y_label)
        self._save_plot(fig, filename, save_formats)
        plt.close(fig)
    
    def _format_plot(self, ax, title: str, x_label: str, y_label: str):
        """Форматирование графика"""
        ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
        ax.set_xlabel(x_label, fontsize=12, labelpad=10)
        ax.set_ylabel(y_label, fontsize=12, labelpad=10)
        ax.grid(True, alpha=0.3)
        ax.legend(loc='best', framealpha=0.9)
        
        # Улучшаем внешний вид
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(axis='both', which='both', length=0)
        ax.set_axisbelow(True)  # Сетка под данными
    
    def _save_plot(self, fig, filename: str, formats: List[str]):
        """Сохранение графика в разных форматах"""
        for fmt in formats:
            filepath = os.path.join(self.output_dir, f"{filename}.{fmt}")
            fig.savefig(filepath, 
                       bbox_inches='tight', 
                       dpi=300,
                       facecolor='white',
                       edgecolor='none')
            print(f"График сохранен: {filepath}")