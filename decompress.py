import os
import shutil
from pathlib import Path

def process_archives(base_dir_path):
    """
    解压 base 文件夹下的所有压缩包，并为解压出的子文件夹加上 _MD&A 后缀。
    """
    base_dir = Path(base_dir_path)

    if not base_dir.exists():
        print(f"❌ 错误：找不到基础文件夹 {base_dir}")
        return

    # Python 内置支持的压缩包格式
    supported_extensions = {'.zip', '.tar', '.gz', '.tgz', '.bz2'}
    
    # 查找 base 目录下的所有文件
    archives = [f for f in base_dir.iterdir() if f.is_file() and f.suffix.lower() in supported_extensions]

    if not archives:
        print(f"⚠️ 在 {base_dir} 中没有找到支持的压缩包。")
        return

    print(f"🚀 发现 {len(archives)} 个压缩包，开始自动化预处理...\n" + "="*40)

    for archive_path in archives:
        print(f"📦 正在处理: {archive_path.name}")

        # 设定解压目标文件夹（以压缩包的名字命名，去掉 .zip 后缀）
        # 例如: 2024.zip 将被解压到 AIWashing/2024/
        extract_dir = base_dir / archive_path.stem

        # 1. 解压操作
        print(f"   ⏳ 正在解压到: {extract_dir.name} ...")
        try:
            # exist_ok=True 避免重复运行时报错
            extract_dir.mkdir(parents=True, exist_ok=True)
            shutil.unpack_archive(str(archive_path), str(extract_dir))
        except Exception as e:
            print(f"   ❌ 解压失败: {e}")
            continue

        # 2. 修改子文件夹名称操作
        print(f"   ⏳ 正在重命名子文件夹...")
        rename_count = 0
        
        # 遍历解压后文件夹内的所有项目
        for sub_item in extract_dir.iterdir():
            # 只对文件夹进行重命名，忽略可能存在的文件
            if sub_item.is_dir():
                # 防重命名保护：如果后缀已经是 _MD&A 则跳过，防止多次运行变成 _MD&A_MD&A
                if not sub_item.name.endswith("_MD&A"):
                    new_name = sub_item.name + "_MD&A"
                    new_path = extract_dir / new_name
                    
                    try:
                        sub_item.rename(new_path)
                        rename_count += 1
                    except Exception as e:
                        print(f"      ⚠️ 重命名 {sub_item.name} 失败: {e}")

        print(f"   ✅ 完成！解压并成功重命名了 {rename_count} 个子文件夹。\n")

    print("🎉 所有压缩包预处理完毕！你的数据已经准备好进入主流水线了。")


if __name__ == "__main__":
    # ================= 基本设置 =================
    # 将这里替换为你的 AIWashing 文件夹绝对路径
    BASE_DIRECTORY = r"D:\PythonProject\AIWashing"  
    # ===========================================

    process_archives(BASE_DIRECTORY)