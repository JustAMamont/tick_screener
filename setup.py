import os
import shutil
import glob
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext as _build_ext 
from Cython.Build import cythonize
import numpy as np

# --- Конфигурация проекта ---
PACKAGE_NAME = "utils"

# Определяем модули расширения
extensions = [
    Extension(
        f"{PACKAGE_NAME}.scanner",
        [os.path.join(PACKAGE_NAME, "scanner.pyx")]
    ),
    Extension(
        f"{PACKAGE_NAME}.cythonic_utils",
        [os.path.join(PACKAGE_NAME, "cythonic_utils.pyx")]
    ),
]

# --- Кастомная команда сборки с автоматической очисткой ---
class build_ext_with_cleanup(_build_ext):
    """
    Переопределяет команду build_ext.
    Сначала запускает стандартную компиляцию, а затем удаляет
    промежуточные .c файлы.
    """
    def run(self):
        # 1. Запускаем оригинальный процесс компиляции
        super().run()

        # 2. После успешной компиляции запускаем очистку .c файлов
        print("-------------------------------------------")
        print("Post-compilation cleanup: removing .c files")
        
        # Находим и удаляем все .c файлы в нашем пакете
        for c_file in glob.glob(os.path.join(PACKAGE_NAME, '*.c')):
            try:
                print(f"Removing intermediate file: {c_file}")
                os.remove(c_file)
            except OSError as e:
                print(f"Error removing file {c_file}: {e}")
        
        # Также чистим в корне, если Cython вдруг создал там что-то
        for c_file in glob.glob('*.c'):
             try:
                print(f"Removing intermediate file from root: {c_file}")
                os.remove(c_file)
             except OSError as e:
                print(f"Error removing file {c_file}: {e}")

        # Удаляем директорию build, она больше не нужна
        if os.path.isdir('build'):
            print("Removing 'build/' directory...")
            shutil.rmtree('build')
            
        print("Cleanup complete. Final .so files are in place.")
        print("-------------------------------------------")

# --- Основная настройка ---
setup(
    ext_modules=cythonize(extensions, language_level="3",
                          compiler_directives={'embedsignature': True}),
    include_dirs=[np.get_include()],
    # Говорим setuptools использовать НАШУ команду вместо стандартной
    cmdclass={
        'build_ext': build_ext_with_cleanup,
    }
)