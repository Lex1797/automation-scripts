import csv
from typing import Iterator, Dict, Any
import logging
from pathlib import Path
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LargeCSVProcessor:
    def __init__(self, input_path: str, output_path: str = None):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path) if output_path else None
        self._validate_paths()

    def _validate_paths(self) -> None:
        """Проверка существования входного файла и доступности выходного пути"""
        if not self.input_path.exists():
            raise FileNotFoundError(f"Input file {self.input_path} not found")
        
        if self.output_path and self.output_path.exists():
            logger.warning(f"Output file {self.output_path} will be overwritten")

    def _row_generator(self) -> Iterator[Dict[str, Any]]:
        """Генератор, который построчно читает CSV и yield'ит словари"""
        with open(self.input_path, mode='r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row_num, row in enumerate(reader, start=1):
                try:
                    yield row
                except Exception as e:
                    logger.error(f"Error processing row {row_num}: {e}")
                    continue

    def process_with_callback(
        self,
        row_callback: callable,
        output_fields: list = None,
        max_rows: int = None
    ) -> None:
        """
        Обработка файла с callback-функцией для каждой строки.
        
        Args:
            row_callback: Функция, принимающая словарь строки и возвращающая
                         обработанный словарь или None для пропуска строки
            output_fields: Список полей для выходного файла
            max_rows: Максимальное количество строк для обработки (для тестов)
        """
        processed_rows = 0
        start_time = time.time()

        with open(self.output_path, mode='w', encoding='utf-8', newline='') as out_file:
            writer = None
            
            for row in self._row_generator():
                if max_rows and processed_rows >= max_rows:
                    break
                
                processed_row = row_callback(row)
                if processed_row is None:
                    continue
                
                # Инициализация writer при первой записи
                if writer is None:
                    output_fields = output_fields or list(processed_row.keys())
                    writer = csv.DictWriter(out_file, fieldnames=output_fields)
                    writer.writeheader()
                
                writer.writerow(processed_row)
                processed_rows += 1
                
                if processed_rows % 10000 == 0:
                    logger.info(f"Processed {processed_rows} rows...")

        elapsed = time.time() - start_time
        logger.info(
            f"Finished processing {processed_rows} rows in {elapsed:.2f} seconds "
            f"({processed_rows/elapsed:.2f} rows/sec)"
        )

# Пример использования
if name == "__main__":
    def example_callback(row: dict) -> dict:
        """Пример callback-функции: фильтрация и преобразование"""
        if float(row['price']) > 1000:
            row['discount'] = float(row['price']) * 0.9
            return row
        return None

    processor = LargeCSVProcessor(
        input_path='data/large_dataset.csv',
        output_path='output/processed_data.csv'
    )
    processor.process_with_callback(
        row_callback=example_callback,
        output_fields=['id', 'name', 'price', 'discount']
    )
