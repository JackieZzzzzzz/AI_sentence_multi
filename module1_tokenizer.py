import re
import jieba
import os
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

# ==========================================
# [核心] 独立出来的单文件处理函数 (分配给子进程干的活)
# ==========================================
def worker_process_file_m1(file_path, save_path, user_dict_path, synonym_dict):
    """
    子进程的工作：接收一个文件，加载必要的词典，分词并写入。
    """
    # 子进程在 Windows 下是独立的内存，需要自己加载自定义词典
    if user_dict_path and os.path.exists(user_dict_path):
        jieba.load_userdict(str(user_dict_path))
        
    re_punc = re.compile(r"[\s+\.\!\/_,$%^*(+\"\'“”《》【】\[\]{}]+|[+——！，。？、~@#￥%……&*（）：；‘’·]+")

    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()

        # 分词与处理
        processed_lines = []
        for segline in re.split(r"[。；;!?！？…]+", text):
            tokens = (w.strip() for w in jieba.cut(segline))
            tokens = [re_punc.sub("", w) for w in tokens if w]
            tokens = [w.lower() if w.isascii() else w for w in tokens if w]
            tokens = [synonym_dict.get(w, w) for w in tokens]
            if tokens:
                processed_lines.append(" ".join(tokens))

        # 集中写入，减少 IO 操作
        with open(save_path, "w", encoding="utf-8") as g:
            g.write("\n".join(processed_lines) + "\n")
            
        return file_path.name, True, "Success"
    except Exception as e:
        return file_path.name, False, str(e)


# ==========================================
# 类的定义保持不变，只重写 run 方法调度多进程
# ==========================================
class MDATokenizer:
    def __init__(self, config):
        self.cfg = config
        self.synonym_dict = {}

    def prepare(self):
        # 主进程依然需要加载一次，主要是为了控制台确认状态
        jieba.load_userdict(str(self.cfg.user_dict))
        jieba.initialize()
        
        if self.cfg.synonym_path.exists():
            with open(self.cfg.synonym_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"): continue
                    parts = re.split(r"\t|\s+", line, maxsplit=1)
                    if len(parts) == 2:
                        self.synonym_dict[parts[0].strip().lower()] = parts[1].strip()
        print(f"✅ [模块1] 已准备就绪: 自定义词典及 {len(self.synonym_dict)} 对同义词。")

    def run(self):
        files = [f for f in self.cfg.src_dir.glob("*.txt") 
                 if not (self.cfg.tok_dir / f.name.replace(".txt", ".token.txt")).exists()]
        
        if self.cfg.test_mode: files = files[:self.cfg.test_n]
        if not files:
            print(f"⏩ [模块1] 所有文件均已分词，跳过执行。")
            return

        # 🚀 启动多进程引擎 (保留 2 个核心给系统，使用 30 个核心)
        max_workers = min(30, os.cpu_count() - 2 if os.cpu_count() else 4)
        print(f"🔥 启动多进程分词引擎，调用核心数: {max_workers}")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # 1. 给每个工人分配任务
            futures = []
            for file_path in files:
                save_path = self.cfg.tok_dir / file_path.name.replace(".txt", ".token.txt")
                future = executor.submit(
                    worker_process_file_m1, 
                    file_path, 
                    save_path, 
                    str(self.cfg.user_dict), 
                    self.synonym_dict
                )
                futures.append(future)

            # 2. 收集结果并显示进度条
            for future in tqdm(as_completed(futures), total=len(files), desc=f"多进程分词 ({self.cfg.year})"):
                filename, success, msg = future.result()
                if not success:
                    print(f"❌ 文件 {filename} 处理失败: {msg}")