mport os
import shutil
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List
import filetype  # Для определения типов файлов

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DocumentOrganizer:
    def __init__(self, source_dir: str, target_dir: str):
        self.source_dir = Path(source_dir)
        self.target_dir = Path(target_dir)
        self._validate_dirs()
        
        # Категории файлов и их расширения
        self.categories = {
            'Documents': ['pdf', 'docx', 'doc', 'txt', 'rtf', 'odt', 'xlsx', 'pptx'],
            'Images': ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg', 'webp'],
            'Archives': ['zip', 'rar', '7z', 'tar', 'gz'],
            'Audio': ['mp3', 'wav', 'ogg', 'flac'],
            'Video': ['mp4', 'mov', 'avi', 'mkv', 'flv'],
            'Code': ['py', 'js', 'html', 'css', 'json', 'xml', 'sql'],
            'Executables': ['exe', 'msi', 'dmg', 'pkg', 'deb'],
        }
        
        self.other_dir = self.target_dir / "Other"

    def _validate_dirs(self) -> None:
        """Проверка существования и доступности директорий"""
        if not self.source_dir.exists():
            raise FileNotFoundError(f"Source directory {self.source_dir} not found")
            
        if not self.target_dir.exists():
            logger.info(f"Creating target directory {self.target_dir}")
            self.target_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_category(self, file_path: Path) -> str:
        """Определение категории файла по его содержимому и расширению"""
        try:
            kind = filetype.guess(file_path)
            if kind is None:
                # Попробуем по расширению, если filetype не определил
                ext = file_path.suffix[1:].lower()
                for category, exts in self.categories.items():
                    if ext in exts:
                        return category
                return "Other"
            
            mime = kind.mime.split('/')[0]
            
            # Сопоставление MIME-типов с нашими категориями
            mime_map = {
                'application': 'Documents',
                'text': 'Documents',
                'image': 'Images',
                'audio': 'Audio',
                'video': 'Video',
                'archive': 'Archives'
            }
            
            return mime_map.get(mime, "Other")
            
        except Exception as e:
            logger.error(f"Error determining file type for {file_path}: {e}")
            return "Other"

    def _get_date_folder(self, file_path: Path) -> str:
        """Получение даты файла для организации по папкам"""
        stat = file_path.stat()
        # Используем дату изменения или создания (что новее)
        timestamp = max(stat.st_mtime, stat.st_ctime)
        file_date = datetime.fromtimestamp(timestamp)
        return file_date.strftime("%Y-%m-%d")

    def _organize_file(self, file_path: Path) -> None:
        """Перемещение файла в соответствующую категорию и дату"""
        if not file_path.is_file():
            return
            
        category = self._get_file_category(file_path)
        date_folder = self._get_date_folder(file_path)
        
        if category == "Other":
            target_path = self.other_dir / date_folder / file_path.name
        else:
            target_path = self.target_dir / category / date_folder / file_path.name
            
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Обработка дубликатов
        counter = 1
        while target_path.exists():
            stem = file_path.stem
            suffix = file_path.suffix
            target_path = target_path.with_name(f"{stem}_{counter}{suffix}")
            counter += 1
            
        try:
            shutil.move(str(file_path), str(target_path))
            logger.info(f"Moved {file_path} to {target_path}")
        except Exception as e:
            logger.error(f"Error moving {file_path}: {e}")

    def organize(self) -> Dict[str, int]:
        """Основной метод организации файлов"""
        file_count = {category: 0 for category in self.categories}
        file_count['Other'] = 0
        
        for item in self.source_dir.rglob('*'):
            if item.is_file():
                category = self._get_file_category(item)
                file_count[category] += 1
                self._organize_file(item)
                
        logger.info("Organization complete. Stats:")
        for category, count in file_count.items():
            if count > 0:
                logger.info(f"{category}: {count} files")
                
        return file_count

# Пример использования
if name == "__main__":
    organizer = DocumentOrganizer(
        source_dir="~/Downloads",
        target_dir="~/Documents/OrganizedFiles"
    )
    
    stats = organizer.organize()
    print("\nOrganization statistics:")
    for category, count in stats.items():
        if count > 0:
            print(f"{category}: {count} files")
