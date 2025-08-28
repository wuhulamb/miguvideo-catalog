#!/usr/bin/env python3
import csv
import math
import json
import os
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import requests

# 常量定义
BASE_URL = "https://jadeite.migu.cn/search/v3/category"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
DEFAULT_TIMEOUT = 10
PAGE_SIZE = 50
MAX_RESULTS = 1500
DATA_DIR = "data"
START_YEAR = 1900
MAX_RETRIES = 3  # 最大重试次数

def fetch_data_with_retry(page_start: int, cont_type: str, media_year: int, retry_count: int = 0) -> Optional[Dict[str, Any]]:
    """发送GET请求获取数据，带有重试机制"""
    params = {
        "pageStart": page_start,
        "pageNum": PAGE_SIZE,
        "contDisplayType": cont_type,
        "mediaYear": media_year
    }

    try:
        response = requests.get(
            BASE_URL,
            params=params,
            headers=DEFAULT_HEADERS,
            timeout=DEFAULT_TIMEOUT
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        if retry_count < MAX_RETRIES:
            print(f"请求失败 (第{page_start}页, 年份{media_year})，第{retry_count+1}次重试...")
            return fetch_data_with_retry(page_start, cont_type, media_year, retry_count + 1)
        else:
            print(f"请求失败 (第{page_start}页, 年份{media_year})，已达到最大重试次数: {e}")
            return None

def extract_data(json_data: Optional[Dict[str, Any]]) -> Tuple[List[Dict[str, str]], int]:
    """从JSON数据中提取所需字段"""
    if not json_data or json_data.get("code") != 200:
        return [], 0

    result_num = json_data.get("resultNum", 0)
    data_list = json_data.get("body", {}).get("data", [])

    if result_num > MAX_RESULTS:
        print(f"结果数量超过 {MAX_RESULTS}，当前为 {result_num}")

    extracted = []
    for item in data_list:
        extracted.append({
            "pID": item.get("pID", ""),
            "name": item.get("name", "").strip(),
            "score": item.get("score", "").strip(),
            "year": item.get("year", "").strip(),
            "contentStyle": item.get("contentStyle", "").strip(),
            "contDisplayName": item.get("contDisplayName", "").strip()
        })
    return extracted, result_num

def save_to_csv(data: List[Dict[str, str]], filename: str) -> None:
    """将数据保存到CSV文件"""
    if not data:
        print("无有效数据可保存")
        return

    try:
        # 确保data目录存在
        os.makedirs(DATA_DIR, exist_ok=True)
        filepath = os.path.join(DATA_DIR, filename)

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                "contDisplayName", "year", "pID", "name", "score", "contentStyle"
            ], lineterminator="\n")
            writer.writeheader()
            writer.writerows(data)
        print(f"成功保存 {len(data)} 条数据到 {filepath}")
    except IOError as e:
        print(f"保存文件失败: {e}")

def load_existing_data(filename: str) -> List[Dict[str, str]]:
    """加载已存在的CSV数据"""
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        return []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        print(f"读取现有数据失败: {e}")
        return []

def merge_data(existing_data: List[Dict[str, str]], new_data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """合并现有数据和新数据，去重"""
    # 创建现有数据的唯一标识集合
    existing_keys = set()
    for item in existing_data:
        key = (item.get('year', ''), item.get('pID', ''))
        existing_keys.add(key)

    # 添加新数据中不重复的条目
    merged_data = existing_data.copy()
    for item in new_data:
        key = (item.get('year', ''), item.get('pID', ''))
        if key not in existing_keys:
            merged_data.append(item)
            existing_keys.add(key)

    return merged_data

def sort_data(data: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """按照(year, pID)排序数据"""
    return sorted(data, key=lambda x: (x.get('year', ''), x.get('pID', '')))

def load_categories(catalog_file: str = 'catalog.json') -> Dict[str, str]:
    """加载分类信息"""
    try:
        with open(catalog_file, 'r', encoding='utf-8') as f:
            catalog = json.load(f)
        return {item['name']: item['type'] for item in catalog.get('contDisplayTypeList', [])}
    except FileNotFoundError:
        print(f"未找到分类文件: {catalog_file}")
        return {}
    except json.JSONDecodeError:
        print(f"分类文件解析错误: {catalog_file}")
        return {}

def process_year_data(year: int, category_code: str) -> List[Dict[str, str]]:
    """处理单个年份的数据抓取"""
    year_data = []
    first_page = fetch_data_with_retry(1, category_code, year)
    if not first_page:
        return year_data

    extracted_first, result_num = extract_data(first_page)
    if not extracted_first:
        print(f"{year} 年没有数据")
        return year_data

    print(f"总结果数: {result_num}")
    total_pages = math.ceil(result_num / PAGE_SIZE)
    year_data.extend(extracted_first)
    print(f"\r已获取第 1 页数据", end="", flush=True)

    for page in range(2, total_pages + 1):
        json_data = fetch_data_with_retry(page, category_code, year)
        if not json_data:
            continue

        extracted, _ = extract_data(json_data)
        if extracted:
            year_data.extend(extracted)
            print(f"\r已获取第 {page} 页数据", end="", flush=True)
        else:
            print(f"{year} 年第 {page} 页无有效数据")
    print()

    return year_data

def process_category(category_name: str, category_code: str, start_year: int, end_year: int) -> None:
    """处理单个类别的数据爬取和合并"""
    filename = f"{category_name}.csv"

    # 加载现有数据
    existing_data = load_existing_data(filename)
    print(f"类别 {category_name} 现有数据条数: {len(existing_data)}")

    # 爬取新数据
    all_new_data = []
    print(f"开始爬取类别: {category_name} (代码: {category_code})")
    print(f"年份范围: {start_year}-{end_year}")

    for year in range(start_year, end_year + 1):
        print(f"=== {year} ===")
        year_data = process_year_data(year, category_code)
        all_new_data.extend(year_data)
        print()

    # 合并数据并去重
    merged_data = merge_data(existing_data, all_new_data)
    print(f"合并后数据条数: {len(merged_data)} (新增: {len(merged_data) - len(existing_data)})")

    # 排序数据
    sorted_data = sort_data(merged_data)

    # 保存数据
    save_to_csv(sorted_data, filename)

def main() -> None:
    categories = load_categories()
    if not categories:
        print("无法加载分类信息，程序退出")
        return

    # 获取当前年份
    current_year = datetime.now().year

    # 确保数据目录存在
    os.makedirs(DATA_DIR, exist_ok=True)

    # 遍历所有类别
    for category_name, category_code in categories.items():
        print(f"\n{'='*50}")
        print(f"处理类别: {category_name}")
        print(f"{'='*50}")

        try:
            process_category(category_name, category_code, START_YEAR, current_year)
        except Exception as e:
            print(f"处理类别 {category_name} 时出错: {e}")
            continue

if __name__ == "__main__":
    main()
