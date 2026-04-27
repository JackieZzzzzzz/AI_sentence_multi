import logging
from gensim.models import Word2Vec
from gensim.models.word2vec import PathLineSentences

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

class Word2VecTrainer:
    def __init__(self, config):
        self.cfg = config

    def run(self):
        print(f"🚀 [模块2] 开始训练 {self.cfg.year} 年度 Word2Vec 模型...")
        sentences = PathLineSentences(str(self.cfg.tok_dir))
        
        # 🚀 workers=30 解锁服务器全部算力
        model = Word2Vec(
            vector_size=200, window=8, min_count=5, sg=1,
            workers=30, seed=42, hashfxn=hash 
        )
        
        # 构建词汇表
        model.build_vocab(sentences)
        
        # 🟢 拦截空语料库，防止误导性报错
        if model.corpus_count == 0:
            print(f"❌ [模块2] 致命错误：读取到的语料为空！")
            print(f"👉 请去检查文件夹 {self.cfg.tok_dir} 是否存在且有实际文本内容。")
            return  # 提前终止
            
        # 开始训练
        model.train(sentences, total_examples=model.corpus_count, epochs=5)
        model.save(str(self.cfg.model_path))
        print("✅ [模块2] 模型训练完成并保存。")

        # 提取相似词
        result = model.wv.most_similar('人工智能', topn=99)
        with open(self.cfg.keyword_file, "w", encoding="utf-8") as f:
            f.write("人工智能\n")
            for word, _ in result:
                f.write(f"{word}\n")
        print(f"✅ [模块2] 前100个AI相关词已保存至 {self.cfg.keyword_file.name}")