import re
import pandas as pd
from tqdm import tqdm

class AIExtractor:
    def __init__(self, config):
        self.cfg = config
        self.keyword_pattern = None

    def prepare(self):
        with open(self.cfg.keyword_file, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        
        sorted_keywords = sorted(set(keywords), key=len, reverse=True)
        self.keyword_pattern = re.compile(r'(' + '|'.join([re.escape(kw) for kw in sorted_keywords]) + r')')
        print(f"✅ [模块4] 已加载并编译 {len(sorted_keywords)} 个提取关键词")

    def run(self):
        self.prepare()
        df_index = pd.read_csv(self.cfg.metadata_file)
        if self.cfg.test_mode: df_index = df_index.head(self.cfg.test_n)

        ai_results = []
        stats_results = []

        for _, row in tqdm(df_index.iterrows(), total=len(df_index), desc=f"句子抽取 ({self.cfg.year})"):
            file_path = self.cfg.sentence_dir / row['File_Path']
            if not file_path.exists(): continue

            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                sentences = [line.strip() for line in f if line.strip()]
            
            stats_results.append({
                "Stock_Code": row['Stock_Code'], "Year": row['Year'], "Total_Sentences": len(sentences)
            })

            for sent in sentences:
                matches = self.keyword_pattern.findall(sent)
                if matches:
                    unique_keywords = list(dict.fromkeys(matches))
                    ai_results.append({
                        "Stock_Code": row['Stock_Code'], "Year": row['Year'],
                        "Sentence": sent, "Extracted_Keyword": "，".join(unique_keywords)
                    })

        pd.DataFrame(ai_results).to_csv(self.cfg.output_sentences, index=False, encoding='utf-8-sig')
        pd.DataFrame(stats_results).to_csv(self.cfg.output_stats, index=False, encoding='utf-8-sig')
        print(f"🎉 [模块4] 抽取完成！结果已存至 {self.cfg.output_sentences.name}")