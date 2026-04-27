from config import PipelineConfig
from module1_tokenizer import MDATokenizer
from module2_w2v import Word2VecTrainer
from module3_sentence_parser import SentenceParser
from module4_extractor import AIExtractor

def process_year(base_dir, year, test_mode=False, test_n=10):
    print(f"\n{'='*20} 🚀 开始处理 {year} 年度数据 {'='*20}")
    
    # 1. 初始化配置并创建文件夹
    cfg = PipelineConfig(base_dir, year, test_mode, test_n)
    cfg.ensure_dirs()

    # 2. 预处理分词 (生成 2024_token)
    tokenizer = MDATokenizer(cfg)
    tokenizer.prepare()
    tokenizer.run()

    # 3. Word2Vec 训练及百词提取 (仅当 result 文件不存在时训练，节省时间)
    if not cfg.keyword_file.exists():
        trainer = Word2VecTrainer(cfg)
        trainer.run()
    else:
        print(f"⏩ [跳过] {cfg.keyword_file.name} 已存在，跳过模型训练。")

    # 4. 元数据、正则替换、分句 (生成 2024_sentence)
    parser = SentenceParser(cfg)
    parser.run()

    # 5. 目标抽取 (生成 最终 CSV)
    extractor = AIExtractor(cfg)
    extractor.run()
    
    print(f"{'='*20} 🎉 {year} 年度数据处理完毕 {'='*20}\n")

if __name__ == "__main__":
    # ================= 基本设置 =================
    BASE_DIRECTORY = r"D:\PythonProject\AIWashing"  # 注意这里只写到外层
    YEARS_TO_PROCESS = [2022]  # 如果以后有2023, 2025，只需在这里加：[2023, 2024, 2025]
    
    # 测试开关
    TEST_MODE = False
    TEST_N = 10
    # ===========================================

    for yr in YEARS_TO_PROCESS:
        process_year(
            base_dir=BASE_DIRECTORY, 
            year=yr, 
            test_mode=TEST_MODE, 
            test_n=TEST_N
        )