import os
import re
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

# ==========================================
# [核心] 独立出来的单文件处理函数
# ==========================================
def worker_process_file_m3(file_path, save_path, synonym_dict, replace_pattern):
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()
        
        # 正则替换同义词
        if replace_pattern:
            text = replace_pattern.sub(lambda m: synonym_dict[m.group(0).lower().strip()], text)
        
        # 抹平换行，彻底分句
        text = text.replace('\n', '').replace('\r', '').replace('\t', ' ')
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'第\d+页|共\d+页', '', text)
        chunks = re.split(r'(?<=[。！？；?.!;])', text)
        sentences = [s.strip() for s in chunks if len(s.strip()) > 5]

        # 集中写入单句
        with open(save_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(sentences) + '\n')
            
        return file_path.name, True, "Success"
    except Exception as e:
        return file_path.name, False, str(e)


# ==========================================
class SentenceParser:
    def __init__(self, config):
        self.cfg = config
        self.synonym_dict = {}
        self.replace_pattern = None

    def build_metadata(self):
        file_list = []
        for file in self.cfg.src_dir.glob("*.txt"):
            parts = file.stem.split('-')
            if len(parts) >= 2:
                file_list.append({
                    "File_Name": file.name, "Stock_Code": parts[0], 
                    "Year": parts[1], "File_Path": file.name, "Processed": 0
                })
        df = pd.DataFrame(file_list)
        df.to_csv(self.cfg.metadata_file, index=False, encoding="utf-8-sig")
        print(f"✅ [模块3] 元数据索引已保存: {len(df)} 条记录")

    def _prepare_regex(self):
        if self.cfg.synonym_path.exists():
            with open(self.cfg.synonym_path, "r", encoding="utf-8") as f:
                for line in f:
                    parts = re.split(r"\t+|\s{2,}", line.strip(), maxsplit=1)
                    if len(parts) == 2:
                        self.synonym_dict[parts[0].strip().lower()] = parts[1].strip()
        sorted_keys = sorted(self.synonym_dict.keys(), key=len, reverse=True)
        escaped_keys = [re.escape(k).replace(' ', r'\s+') for k in sorted_keys]
        if escaped_keys:
            self.replace_pattern = re.compile(r"(?<![a-zA-Z])(" + "|".join(escaped_keys) + r")(?![a-zA-Z])", re.IGNORECASE)

    def run(self):
        self.build_metadata()
        self._prepare_regex()

        files = [f for f in self.cfg.src_dir.glob("*.txt") 
                 if not (self.cfg.sentence_dir / f.name).exists()]
        if self.cfg.test_mode: files = files[:self.cfg.test_n]
        if not files:
            print(f"⏩ [模块3] 所有文件均已分句，跳过执行。")
            return

        # 🚀 启动多进程引擎
        max_workers = min(30, os.cpu_count() - 2 if os.cpu_count() else 4)
        print(f"🔥 启动多进程分句引擎，调用核心数: {max_workers}")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for file_path in files:
                save_path = self.cfg.sentence_dir / file_path.name
                future = executor.submit(
                    worker_process_file_m3, 
                    file_path, 
                    save_path, 
                    self.synonym_dict, 
                    self.replace_pattern
                )
                futures.append(future)

            for future in tqdm(as_completed(futures), total=len(files), desc=f"多进程分句 ({self.cfg.year})"):
                filename, success, msg = future.result()
                if not success:
                    print(f"❌ 文件 {filename} 处理失败: {msg}")