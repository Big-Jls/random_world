import os
import csv
import json
import random
import logging
from typing import List, Dict, Set, Union
from concurrent.futures import ThreadPoolExecutor  # 可选多线程
from nbtlib import load as nbt_load, File, Compound, List as NBTList, schema
from nbtlib.tag import String, Int
import shutil
import zipfile
from datetime import datetime

# import gzip
# import magic

# ====================
# 基础配置
# ====================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(name)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
# 可选黑名单
BLACKLIST = {
    'bedrock', 'trial_spawner', 'spawner', 'command_block', 'water', 'lava', 'white_candle', 'light_gray_candle',
    'gray_candle','anvil', 'chipped_anvil', 'damaged_anvil', 'ladder', 'scaffolding',
    'black_candle', 'brown_candle', 'red_candle', 'orange_candle', 'yellow_candle', 'lime_candle',
    'green_candle', 'cyan_candle', 'light_blue_candle', 'blue_candle', 'purple_candle', 'magenta_candle',
    'pink_candle', 'white_bed', 'light_gray_bed', 'gray_bed',
    'black_bed', 'brown_bed', 'red_bed', 'orange_bed', 'yellow_bed', 'lime_bed',
    'green_bed', 'cyan_bed', 'light_blue_bed', 'blue_bed', 'purple_bed', 'magenta_bed',
    'pink_bed', 'white_carpet', 'light_gray_carpet', 'gray_carpet',
    'black_carpet', 'brown_carpet', 'red_carpet', 'orange_carpet', 'yellow_carpet', 'lime_carpet',
    'green_carpet', 'cyan_carpet', 'light_blue_carpet', 'blue_carpet', 'purple_carpet', 'magenta_carpet',
    'pink_carpet', 'white_banner', 'light_gray_banner', 'gray_banner',
    'black_banner', 'brown_banner', 'red_banner', 'orange_banner', 'yellow_banner', 'lime_banner',
    'green_banner', 'cyan_banner', 'light_blue_banner', 'blue_banner', 'purple_banner', 'magenta_banner',
    'pink_banner', 'iron_door', 'copper_door', 'exposed_copper_door', 'weathered_copper_door', 'waxed_copper_door',
    'oxidized_copper_door', 'waxed_exposed_door', 'waxed_weathered_copper_door',
    'waxed_oxidized_copper_door', 'oak_button', 'birch_button', 'spruce_button', 'jungle_button', 'acacia_button',
    'dark_oak_button', 'mangrove_button', 'cherry_button', 'pale_oak_button', 'bamboo_button', 'crimson_button',
    'warped_button', 'stone_button', 'polished_blackstone_button', 'oak_door', 'birch_door', 'spruce_door',
    'jungle_door', 'acacia_door', 'dark_oak_door', 'mangrove_door', 'cherry_door', 'pale_oak_door', 'bamboo_door',
    'crimson_door', 'warped_door', 'snow', 'moss_carpet', 'pale_moss_carpet', 'pale_hanging_moss', 'pointed_dripstone',
    'oak_sapling', 'birch_sapling', 'spruce_sapling', 'jungle_sapling', 'acacia_sapling', 'dark_oak_sapling',
    'mangrove_propagule', 'cherry_sapling', 'pale_oak_sapling', 'azalea', 'flowering_azalea', 'brown_mushroom',
    'red_mushroom', 'crimson_fungus', 'warped_fungus', 'short_grass', 'fern', 'dead_bush', 'dandelion', 'poppy',
    'blue_orchid', 'allium', 'azure_bluet', 'red_tulip', 'orange_tulip', 'white_tulip', 'pink_tulip', 'oxeye_daisy',
    'cornflower', 'lily_of_the_valley', 'torchflower', 'closed_eyeblossom', 'open_eyeblossom', 'wither_rose',
    'pink_petals', 'spore_blossom', 'bamboo', 'sugar_cane', 'cactus', 'crimson_roots', 'warped_roots', 'weeping_vines',
    'twisting_vines', 'vine', 'tall_grass', 'large_fern', 'sunflower', 'lilac', 'rose_bush', 'peony', 'pitcher_plant',
    'big_dripleaf', 'small_dripleaf', 'glow_lichen', 'hanging_roots', 'glow_berries', 'sweet_berries', 'nether_wart',
    'lily_pad', 'seagrass', 'sea_pickle', 'kelp', 'tube_coral', 'brain_coral', 'bubble_coral',
    'fire_coral', 'horn_coral', 'tube_coral_fan', 'brain_coral_fan', 'bubble_coral_fan',
    'fire_coral_fan', 'horn_coral_fan', 'dead_tube_coral', 'dead_brain_coral', 'dead_bubble_coral',
    'dead_fire_coral', 'dead_horn_coral', 'dead_tube_coral_fan', 'dead_brain_coral_fan', 'dead_bubble_coral_fan',
    'dead_fire_coral_fan', 'dead_horn_coral_fan', 'torch', 'soul_torch', 'redstone_torch', 'oak_sign', 'birch_sign',
    'spruce_sign', 'jungle_sign', 'acacia_sign', 'dark_oak_sign', 'mangrove_sign', 'cherry_sign', 'pale_oak_sign',
    'bamboo_sign', 'vault', 'redstone', 'repeater', 'comparator', 'lever', 'oak_pressure_plate', 'birch_pressure_plate',
    'spruce_pressure_plate', 'jungle_pressure_plate', 'acacia_pressure_plate', 'dark_oak_pressure_plate', 'warped_plate'
    'mangrove_pressure_plate', 'cherry_pressure_plate', 'pale_oak_pressure_plate', 'bamboo_pressure_plate',
    'light_weighted_pressure_plate', 'heavy_weighted_pressure_plate', 'polished_blackstone_pressure_plate',
    'tripwire_hook', 'rail', 'powered_rail', 'detector_rail', 'activator_rail', 'stone_pressure_plate'
}
# 可选优先级权重
PRIORITY_BLOCKS = {'iron_block': 10}


# ====================
# 新增日志配置
# ====================
def setup_logging():
    """增强型日志配置"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # 总日志级别为DEBUG

    # 文件处理器（记录DEBUG及以上）
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log"),
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)

    # 控制台处理器（仅记录INFO及以上）
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '[%(asctime)s] %(name)s - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# ====================
# 主处理类
# ====================
class MinecraftWorldRandomizer:
    def __init__(self, block_csv: str = 'block_id.csv'):
        setup_logging()
        self.block_ids = self._load_block_ids(block_csv)
        self.reset_count = 0
        self.logger = logging.getLogger('Random_World')
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.executor = ThreadPoolExecutor(max_workers=4)
        self._shutdown_registered = False  # 新增状态标记

    def wait_completion(self):
        """等待所有任务完成"""
        self.logger.info("等待线程池任务完成...")
        self.executor.shutdown(wait=True)
        self.logger.info(f"全部任务完成 | 重置次数: {self.reset_count}")

    def _load_block_ids(self, csv_path: str) -> List[str]:
        """加载并预处理方块ID（含完整性检查）"""
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                ids = []
                reader = csv.DictReader(f)

                if 'id' not in reader.fieldnames:
                    self.logger.critical("CSV文件缺少'id'列")
                    raise ValueError("无效的CSV格式")

                for row in reader:
                    block_id = row['id'].strip()
                    if not block_id:
                        continue

                    if block_id in BLACKLIST:
                        continue

                    repeat = PRIORITY_BLOCKS.get(block_id, 1)
                    ids.extend([block_id] * repeat)

                if not ids:
                    self.logger.critical("方块ID池为空！请检查：")
                    self.logger.critical(f"1. CSV文件路径: {os.path.abspath(csv_path)}")
                    self.logger.critical("2. 黑名单是否过滤过多")
                    raise ValueError("无效的方块ID池")

                random.shuffle(ids)
                return ids

        except FileNotFoundError:
            self.logger.critical(f"方块ID文件不存在: {csv_path}")
            raise

    def _get_random_id(self, used_set: Set[str]) -> str:
        """优化的随机ID生成器"""
        available = [id for id in self.block_ids if id not in used_set]

        if not available:
            self.logger.warning("本地ID池耗尽，允许重复使用")
            used_set.clear()
            available = self.block_ids.copy()
            self.reset_count += 1

        chosen = random.choice(available)
        used_set.add(chosen)
        return f"minecraft:{chosen}"

    # ====================
    # 文件处理逻辑
    # ====================
    def randomize_worldgen(self, worldgen_path: str):
        """处理世界生成配置"""
        self._process_directory(worldgen_path, self._process_json_file)

    def randomize_structures(self, structures_path: str):
        """处理结构文件"""
        self._process_directory(structures_path, self._process_nbt_file)

    def _process_directory(self, path: str, processor: callable):
        """安全的目录处理器"""
        if not os.path.exists(path):
            self.logger.error(f"路径不存在: {path}")
            return

        for root, _, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                self.executor.submit(self._safe_process, processor, file_path)

    def _safe_process(self, processor: callable, file_path: str):
        """带异常捕获的处理"""
        try:
            processor(file_path)
        except Exception as e:
            self.logger.error(f"处理失败 [{file_path}]: {str(e)}", exc_info=True)

    # ====================
    # JSON处理
    # ====================
    def _process_json_file(self, file_path: str):
        if not file_path.endswith('.json'):
            return

        try:
            with open(file_path, 'r+', encoding='utf-8') as f:
                data = json.load(f)
                local_used = set()
                self._modify_json(data, local_used, file_path)  # 关键点：传递file_path
                f.seek(0)
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.truncate()
                self.logger.info(f"处理完成: {file_path}")

        except Exception as e:
            self.logger.error(f"JSON处理失败 [{file_path}]: {str(e)}")

    def _modify_json(self, data: Union[dict, list], used_set: Set[str], file_path: str):
        """递归修改JSON数据并记录日志"""
        if isinstance(data, dict):
            for key in list(data.keys()):
                if key in ["Name", "name"]:
                    original = data[key]
                    if original in ['minecraft:water', 'minecraft:lava']:
                        continue
                    new_id = self._get_random_id(used_set)
                    self.logger.debug(f"JSON替换 | 文件: {file_path} | 原始: {original} → 新: {new_id}")
                    data[key] = new_id
                else:
                    # 递归传递所有参数
                    self._modify_json(data[key], used_set, file_path)  # 传递file_path
        elif isinstance(data, list):
            for item in data:
                # 递归传递所有参数
                self._modify_json(item, used_set, file_path)  # 传递file_path

    # ====================
    # NBT处理
    # ====================
    def _process_nbt_file(self, file_path: str):
        if not file_path.endswith('.nbt'):
            return

        try:
            normalized_path = os.path.normpath(file_path)
            if not os.path.exists(normalized_path):
                self.logger.error(f"文件不存在: {normalized_path}")
                return

            is_gzipped = self._detect_gzip(normalized_path)
            with nbt_load(normalized_path, gzipped=is_gzipped) as nbt_data:
                # 新增NBT定位日志
                palette, found_path = self._find_palette_with_path(nbt_data)
                if palette:
                    self.logger.debug(f"定位到NBT: {found_path}")
                    self._modify_palette(palette, normalized_path)
                    # 关键修复：指定压缩模式保存
                    nbt_data.save(gzipped=is_gzipped)  # 新增参数
                    self.logger.info(f"成功保存: {normalized_path}")
                else:
                    self.logger.warning(f"未找到NBT: {normalized_path}")

        except Exception as e:
            self.logger.error(f"NBT处理失败 [{normalized_path}]: {str(e)}", exc_info=True)

    def _detect_gzip(self, file_path: str) -> bool:
        """增强型GZIP检测"""
        try:
            # 使用更可靠的检测方式
            with open(file_path, 'rb') as f:
                header = f.read(3)
                # 检查标准GZIP头 (1F 8B 08)
                return header.startswith(b'\x1f\x8b\x08')
        except Exception as e:
            self.logger.error(f"GZIP检测失败 [{file_path}]: {str(e)}")
            return False

    def _find_palette_with_path(self, node: Union[Compound, NBTList], current_path: str = '') -> (Union[NBTList, None], str):
        """增强型NBT定位方法，支持多层嵌套结构"""
        # 基础情况：当前节点是包含NBT的Compound
        if isinstance(node, Compound):
            if 'palette' in node and isinstance(node['palette'], NBTList):
                return node['palette'], f"{current_path}/palette"

            # 深度优先搜索所有子节点
            for key in node.keys():
                child_node = node[key]
                result, found_path = self._find_palette_with_path(child_node, f"{current_path}/{key}")
                if result:
                    return result, found_path

        # 处理列表嵌套的情况 (如[[palette], ...])
        elif isinstance(node, NBTList):
            # 遍历列表的每个元素，无论其类型
            for i, item in enumerate(node):
                # 构建索引路径 (如 , [1] 等)
                indexed_path = f"{current_path}[{i}]"

                # 递归处理列表元素
                result, found_path = self._find_palette_with_path(item, indexed_path)
                if result:
                    return result, found_path

        # 未找到NBT
        return None, current_path

    def _modify_palette(self, palette: NBTList, file_path: str):
        """安全的NBT修改"""
        local_used = set()
        modified = 0
        skipped = {'minecraft:water', 'minecraft:lava'}

        for idx, block in enumerate(palette):
            try:
                if not isinstance(block, Compound):
                    continue

                name_tag = block.get('Name')
                if not name_tag:
                    continue

                original = str(name_tag)
                if original in skipped:
                    continue

                new_name = self._get_random_id(local_used)
                block['Name'] = String(new_name)
                modified += 1

            except Exception as e:
                self.logger.error(f"方块处理失败 [{file_path} 索引{idx}]: {str(e)}")

        total = len(palette)
        self.logger.info(f"修改完成 [{file_path}]: {modified}/{total}")

    # # ====================
    # # 清理资源
    # # ====================
    # def __del__(self):
    #     self.executor.shutdown(wait=False)
    #     self.logger.info(f"处理完成 | 重置次数: {self.reset_count}")


# ====================
# 数据包打包
# ====================
class WorldPackager:
    def __init__(self, output_name: str = "random_world"):
        self.output_name = output_name
        self.logger = logging.getLogger('Packager')

    def create_pack(self):
        """创建完整数据包"""
        temp_dir = "temp_pack"
        try:
            # 创建基础目录结构
            os.makedirs(os.path.join(temp_dir, "data/minecraft"), exist_ok=True)
            self.logger.info(f"创建临时目录结构: {temp_dir}")

            # ====================
            # 1. 添加数据包图标
            # ====================
            icon_src = "pack.png"
            icon_dst = os.path.join(temp_dir, "pack.png")
            if os.path.exists(icon_src):
                shutil.copy(icon_src, icon_dst)
                self.logger.info(f"数据包图标已添加: {icon_src} → {icon_dst}")
            else:
                self.logger.warning("未找到数据包图标文件 pack.png")

            # ====================
            # 2. 复制游戏数据文件
            # ====================
            # 复制 loot_table 文件
            self._copy_dir(
                src_dir='blocks',
                dst_dir=os.path.join(temp_dir, "data/minecraft/loot_table/blocks")
            )

            # 复制 worldgen 配置
            self._copy_dir(
                src_dir='nbt_dir/worldgen',
                dst_dir=os.path.join(temp_dir, "data/minecraft/worldgen")
            )

            # 复制结构文件
            self._copy_dir(
                src_dir='nbt_dir/structure',
                dst_dir=os.path.join(temp_dir, "data/minecraft/structure")
            )

            # ====================
            # 3. 生成元数据文件
            # ====================
            self._create_pack_meta(temp_dir)

            # ====================
            # 4. 打包为ZIP文件
            # ====================
            self._create_zip(temp_dir)

        except Exception as e:
            self.logger.error(f"打包过程中出现错误: {str(e)}", exc_info=True)
            raise
        finally:
            # 清理临时文件
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                self.logger.info(f"已清理临时目录: {temp_dir}")

    def _copy_dir(self, src_dir: str, dst_dir: str):
        """安全的目录复制操作"""
        if not os.path.exists(src_dir):
            self.logger.warning(f"跳过不存在的源目录: {src_dir}")
            return

        try:
            shutil.copytree(
                src=src_dir,
                dst=dst_dir,
                dirs_exist_ok=True,
                ignore=shutil.ignore_patterns('*.bak', '*.tmp')  # 忽略临时文件
            )
            self.logger.info(f"目录已复制: {src_dir} → {dst_dir}")
        except Exception as e:
            self.logger.error(f"目录复制失败 [{src_dir}]: {str(e)}")
            raise

    def _create_pack_meta(self, temp_dir: str):
        """生成 pack.mcmeta 文件"""
        meta_data = {
            "pack": {
                "pack_format": 61,  # 对应 1.20.5 版本
                "description": "Random_World - Generated by Big_Jls & DeepSeek"
            }
        }
        meta_path = os.path.join(temp_dir, "pack.mcmeta")
        
        try:
            with open(meta_path, 'w', encoding='utf-8') as f:
                json.dump(meta_data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"元数据文件已生成: {meta_path}")
        except Exception as e:
            self.logger.error(f"元数据文件创建失败: {str(e)}")
            raise

    def _create_zip(self, temp_dir: str):
        """打包为 ZIP 文件"""
        zip_path = f"{self.output_name}.zip"
        
        try:
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # 优先添加根目录文件
                root_files = ['pack.mcmeta', 'pack.png']
                for file in root_files:
                    src = os.path.join(temp_dir, file)
                    if os.path.exists(src):
                        zipf.write(src, arcname=file)
                        self.logger.debug(f"已添加根文件: {file}")

                # 添加其他目录结构
                for root, dirs, files in os.walk(temp_dir):
                    # 跳过已处理的根目录文件
                    if root == temp_dir:
                        files = [f for f in files if f not in root_files]

                    for file in files:
                        full_path = os.path.join(root, file)
                        arcname = os.path.relpath(full_path, temp_dir)
                        zipf.write(full_path, arcname=arcname)
                        self.logger.debug(f"打包文件: {arcname}")

            self.logger.info(f"数据包已生成: {zip_path} (大小: {os.path.getsize(zip_path)/1024:.1f}KB)")
        except Exception as e:
            self.logger.error(f"ZIP 打包失败: {str(e)}")
            raise


# ====================
# 使用示例
# ====================
if __name__ == '__main__':
    try:
        randomizer = MinecraftWorldRandomizer(block_csv='block_id.csv')
        randomizer.randomize_worldgen('./blocks')
        randomizer.randomize_worldgen('./nbt_dir/worldgen')
        randomizer.randomize_structures('./nbt_dir/structure')

        randomizer.wait_completion()  # 新增：等待所有文件处理完成

        packager = WorldPackager()
        packager.create_pack()  # 此时文件已修改完成

    except Exception as e:
        logging.critical(f"主流程错误: {str(e)}", exc_info=True)
