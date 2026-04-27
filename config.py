import os
from pathlib import Path

class PipelineConfig:
    def __init__(self, base_dir, year, test_mode=False, test_n=10):
        self.base_dir = Path(base_dir)
        self.year = str(year)
        self.test_mode = test_mode
        self.test_n = test_n

        # 工作目录 (例如 D:\PythonProject\AIWashing\2024)
        self.work_dir = self.base_dir / self.year

        # 输入输出文件夹
        self.src_dir = self.work_dir / f"{self.year}_MD&A"
        self.tok_dir = self.work_dir / f"{self.year}_token"
        self.replace_dir = self.work_dir / f"{self.year}_replace"
        self.sentence_dir = self.work_dir / f"{self.year}_sentence"

        # 字典与模型文件
        self.user_dict = self.base_dir / "AI_dictionary_merged.txt"
        self.synonym_path = self.base_dir / "synonym.txt"
        self.model_path = self.work_dir / f"Model{self.year}.model"
        self.keyword_file = self.work_dir / f"result{self.year}.txt"
        self.metadata_file = self.work_dir / f"metadata_{self.year}.csv"
        
        # 结果输出
        self.output_sentences = self.work_dir / f"step3_ai_sentences_detail_{self.year}.csv"
        self.output_stats = self.work_dir / f"step3_report_stats_{self.year}.csv"

    def ensure_dirs(self):
        """确保所有输出目录存在"""
        for d in [self.tok_dir, self.replace_dir, self.sentence_dir]:
            d.mkdir(parents=True, exist_ok=True)