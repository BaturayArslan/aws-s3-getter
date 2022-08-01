from typing import List, Tuple, Optional, Union, Dict
from configparser import RawConfigParser
import os
from collections import defaultdict

class Configs:
    def __init__(self):
        self._file_path: Optional[str] = None
        self.cparser = RawConfigParser()

    def get_sections(self) -> List[str]:
        return self.cparser.sections()

    def get_section(self, section: str) -> List[Tuple[str, str]]:
        return self.cparser.items(section)

    def get(self, section: str, option: str) -> str:
        return self.cparser.get(section, option)

    def has_option(self, section: str, setting: str) -> bool:
        return self.cparser.has_option(section, setting)

    def set_file_path(self, file_path: str) -> None:
        if self._check_path(file_path):
            if not file_path.startswith("/"):
                current_directory = os.getcwd()
                absolute_path = os.path.join(current_directory, file_path)
                self._file_path = absolute_path
            else:
                self._file_path = file_path
            self.read()
        else:
            raise Exception("File Doesnt Exist")

    def read(self):
        self.cparser.read(self._file_path)

    def _check_path(self, path: str) -> bool:
        if path.startswith("/"):
            return os.path.isfile(path)
        else:
            current_directory = os.getcwd()
            absolute_path = os.path.join(current_directory, path)
            return os.path.isfile(absolute_path)

    def get_configs(self) -> Dict:
        file_name = "./config.ini"

        self.set_file_path(file_name)
        section = self.get_section("filters")
        configs = defaultdict(str)
        for key, value in section:
            configs[key] = value
        return configs


config = Configs().get_configs()
