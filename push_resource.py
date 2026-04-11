import pandas as pd
import requests
import json
import os
import time
import random
from pathlib import Path
from collections import defaultdict

# -------------------------- 从环境变量读取配置（适配GitHub Actions） --------------------------
WECHAT_WEBHOOK = os.getenv("WECHAT_WEBHOOK")  # 从GitHub Secrets读取
EXCEL_FILE_PATH = os.getenv("EXCEL_FILE_PATH", "共享无偿资料库.xlsx")  # 默认值
# 改为读取「标签」列（替换原TARGET_COL_TYPE）
TARGET_COL_TAG = os.getenv("TARGET_COL_TAG", "标签")  
TARGET_COL_NAME = os.getenv("TARGET_COL_NAME", "资源名称")
TARGET_COL_LINK = os.getenv("TARGET_COL_LINK", "资源链接")
# 随机推送的数量（可通过环境变量配置）
RANDOM_PUSH_COUNT = int(os.getenv("RANDOM_PUSH_COUNT", 10))  
SEND_INTERVAL = int(os.getenv("SEND_INTERVAL", 2))

# 指定要优先推送的标签（只要行标签包含其中任意一个，就纳入候选）
# 指定要优先推送的标签
SPECIFIED_TAGS = ["学龄儿童家长", "K12", "技能入门"]


def read_excel_and_filter_by_tags(file_path, col_tag, col_name, col_link, specified_tags):
    """
    读取Excel，筛选出标签包含指定优先标签的行，返回候选资源列表
    :param file_path: Excel路径
    :param col_tag: 标签列名
    :param col_name: 资源名称列名
    :param col_link: 资源链接列名
    :param specified_tags: 优先推送的标签列表
    :return: 候选资源列表 [(名称, 链接, 标签字符串), ...]
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Excel文件不存在：{str(file_path.absolute())}")

    try:
        df = pd.read_excel(
            file_path,
            dtype=str,
            keep_default_na=False,
            na_filter=False
        )
    except Exception as e:
        raise Exception(f"读取Excel失败：{str(e)}")

    # 清洗列名 + 校验必填列
    df.columns = [str(col).strip() for col in df.columns]
    required_cols = [col_tag.strip(), col_name.strip(), col_link.strip()]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise Exception(f"Excel缺少指定列：{missing_cols}，当前表头：{list(df.columns)}")

    # 重命名列 + 数据清洗
    df = df.rename(columns={
        col_tag.strip(): "tag",
        col_name.strip(): "name",
        col_link.strip(): "link"
    })
    df["tag"] = df["tag"].str.strip()
    df["name"] = df["name"].str.strip().replace("", "无名称")
    df["link"] = df["link"].str.strip()

    # 过滤有效数据（链接非空且是http开头、标签非空）
    filter_condition = (
            df["tag"].notna() &
            df["link"].notna() &
            df["link"].str.startswith(("http://", "https://"), na=False)
    )
    df_clean = df[filter_condition].copy()

    if len(df_clean) == 0:
        raise Exception("Excel中没有有效的资源数据")

    # 核心逻辑：筛选标签包含指定优先标签的行
    def is_match_specified_tags(tag_str):
        """判断标签字符串（/分割）是否包含任意一个指定标签"""
        if not tag_str:
            return False
        # 分割标签并去重
        tag_list = [t.strip() for t in tag_str.split("/") if t.strip()]
        # 检查是否有交集
        return len(set(tag_list) & set(specified_tags)) > 0

    # 应用筛选条件
    df_filtered = df_clean[df_clean["tag"].apply(is_match_specified_tags)]

    if len(df_filtered) == 0:
        raise Exception(f"没有找到包含指定优先标签 {specified_tags} 的资源数据")

    # 转换为列表返回
    return [(row["name"], row["link"], row["tag"]) for _, row in df_filtered.iterrows()]


def format_random_resources_message(resources):
    """格式化随机选取的资源消息"""
    if not resources:
        return "⚠️ 没有有效的资源数据"

    # 格式化随机选取的内容
    res_str = "\n".join([
        #f"📚{i + 1}. {name}（标签：{tag}）：\n{link}" 
        f"📚{i + 1}. {name}：\n{link}" 
        for i, (name, link, tag) in enumerate(resources)
    ])

    # 构造消息
    msg_parts = [
        f"🎲 优先推送包含指定标签的资源（共{len(resources)}个）：\n{res_str}\n",
        "💡 🌈点⬆⬆⬆🔗，左下角保存网盘，下载夸克App，(手机端)赠送1T空间‼资源持续更新"
    ]
    final_msg = "\n".join(msg_parts)
    return final_msg[:4000]  # 预留空间，避免超企业微信字符限制


def send_to_wechat_bot(webhook, content, res_type):
    """推送企业微信（复用原逻辑，仅调整日志）"""
    if not webhook or not content:
        raise ValueError("Webhook地址或推送内容不能为空")

    payload = {
        "msgtype": "text",
        "text": {
            "content": content,
            "mentioned_list": [],
            "mentioned_mobile_list": []
        }
    }

    try:
        headers = {"Content-Type": "application/json; charset=utf-8"}
        response = requests.post(
            url=webhook,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            timeout=20,
            verify=False  # GitHub ActionsUbuntu环境SSL兼容
        )
        response.encoding = "utf-8"
        result = response.json()
        if result.get("errcode") != 0:
            raise Exception(f"推送失败：{result.get('errmsg')}（错误码：{result.get('errcode')}）")
        print(f"✅ 【{res_type}】推送成功！")
        return True
    except Exception as e:
        raise Exception(f"推送失败：{str(e)}")


if __name__ == "__main__":
    """主入口：仅保留「优先标签筛选+随机推送」逻辑"""
    try:
        # 校验Webhook是否配置
        if not WECHAT_WEBHOOK:
            raise Exception("❌ 未配置企业微信Webhook（请检查GitHub Secrets）")

        print(f"📌 开始读取Excel文件，筛选包含指定标签 {SPECIFIED_TAGS} 的资源...")
        # 1. 读取并筛选符合条件的资源
        candidate_resources = read_excel_and_filter_by_tags(
            EXCEL_FILE_PATH,
            TARGET_COL_TAG,
            TARGET_COL_NAME,
            TARGET_COL_LINK,
            SPECIFIED_TAGS
        )
        print(f"✅ 筛选完成，共找到 {len(candidate_resources)} 个符合条件的资源")

        # 2. 随机抽取指定数量的资源（不足则取全部）
        push_count = min(RANDOM_PUSH_COUNT, len(candidate_resources))
        random_resources = random.sample(candidate_resources, push_count)
        print(f"✅ 随机选取 {push_count} 个资源准备推送")

        # 3. 格式化消息
        msg_content = format_random_resources_message(random_resources)
        print(f"📝 待推送内容：\n{msg_content}")

        # 4. 推送至企业微信
        send_to_wechat_bot(WECHAT_WEBHOOK, msg_content, "优先标签资源")

        print("\n🎉 优先标签资源推送完成！")

    except Exception as e:
        print(f"❌ 执行失败：{str(e)}")
        exit(1)  # 非0退出码，GitHub Actions会标记为失败
