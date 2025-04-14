import os
import sys
import tempfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
from typing import List, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, date
from functools import wraps
import logging
from contextlib import redirect_stdout, redirect_stderr
import argparse

import yt_dlp
import openpyxl
from openpyxl.styles import Alignment, PatternFill
from moviepy.video.io.VideoFileClip import VideoFileClip


VIDEO_EXTENSIONS = [".mp4", ".mkv", ".avi", ".mov", ".flv"]
EXCEL_HEADERS = ["Video Name", "Channel", "Link", "Status", "Log"]
HEIGHT = [4320, 2160, 1440, 1080, 720, 480, 360, 240, 144]
V_CODEC = ['av1', 'h264', 'vp9.2', 'vp9']
FPS = [60, 30]

VIDEO_FORMATS = {
    '694': {'height': 144,  'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '695': {'height': 240,  'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '696': {'height': 360,  'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '697': {'height': 480,  'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '698': {'height': 720,  'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '699': {'height': 1080, 'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '700': {'height': 1440, 'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '701': {'height': 2160, 'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '702': {'height': 4320, 'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},

    '398': {'height': 720,  'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '399': {'height': 1080, 'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '400': {'height': 1440, 'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '401': {'height': 2160, 'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},
    '402': {'height': 4320, 'fps': 60, 'v_codec': 'av1', 'container': 'mp4'},

    '330': {'height': 144,  'fps': 30, 'v_codec': 'vp9.2', 'container': 'webm'},
    '331': {'height': 240,  'fps': 30, 'v_codec': 'vp9.2', 'container': 'webm'},
    '332': {'height': 360,  'fps': 30, 'v_codec': 'vp9.2', 'container': 'webm'},
    '333': {'height': 480,  'fps': 30, 'v_codec': 'vp9.2', 'container': 'webm'},
    '334': {'height': 720,  'fps': 30, 'v_codec': 'vp9.2', 'container': 'webm'},
    '335': {'height': 1080, 'fps': 30, 'v_codec': 'vp9.2', 'container': 'webm'},
    '336': {'height': 1440, 'fps': 30, 'v_codec': 'vp9.2', 'container': 'webm'},
    '337': {'height': 2160, 'fps': 30, 'v_codec': 'vp9.2', 'container': 'webm'},

    '299': {'height': 1080, 'fps': 60, 'v_codec': 'h264', 'container': 'mp4'},
    '298': {'height': 720,  'fps': 60, 'v_codec': 'h264', 'container': 'mp4'},
    '137': {'height': 1080, 'fps': 30, 'v_codec': 'h264', 'container': 'mp4'},
    '136': {'height': 720,  'fps': 30, 'v_codec': 'h264', 'container': 'mp4'},
    '135': {'height': 480,  'fps': 30, 'v_codec': 'h264', 'container': 'mp4'},
    '134': {'height': 360,  'fps': 30, 'v_codec': 'h264', 'container': 'mp4'},
    '133': {'height': 240,  'fps': 30, 'v_codec': 'h264', 'container': 'mp4'},
    '160': {'height': 144,  'fps': 30, 'v_codec': 'h264', 'container': 'mp4'},

    '247': {'height': 720,  'fps': 30, 'v_codec': 'vp9', 'container': 'webm'},
    '244': {'height': 480,  'fps': 30, 'v_codec': 'vp9', 'container': 'webm'},
    '243': {'height': 360,  'fps': 30, 'v_codec': 'vp9', 'container': 'webm'},
    '242': {'height': 240,  'fps': 30, 'v_codec': 'vp9', 'container': 'webm'},
    '278': {'height': 144,  'fps': 30, 'v_codec': 'vp9', 'container': 'webm'},
    '303': {'height': 1080, 'fps': 60, 'v_codec': 'vp9', 'container': 'webm'},
    '302': {'height': 720,  'fps': 60, 'v_codec': 'vp9', 'container': 'webm'},
    '308': {'height': 1440, 'fps': 60, 'v_codec': 'vp9', 'container': 'webm'},
    '315': {'height': 2160, 'fps': 60, 'v_codec': 'vp9', 'container': 'webm'},
    '272': {'height': 4320, 'fps': 60, 'v_codec': 'vp9', 'container': 'webm'},
}

AUDIO_FORMATS = {
    '139': {'abr': 48,  'a_codec': 'aac',   'container': 'mp4'},
    '140': {'abr': 128, 'a_codec': 'aac',   'container': 'mp4'},
    '141': {'abr': 256, 'a_codec': 'aac',   'container': 'mp4'},

    '249': {'abr': 50,  'a_codec': 'opus',  'container': 'webm'},
    '250': {'abr': 70,  'a_codec': 'opus',  'container': 'webm'},
    '251': {'abr': 128, 'a_codec': 'opus',  'container': 'webm'},

    '256': {'abr': 192, 'a_codec': 'aac',   'container': 'mp4'},
    '258': {'abr': 384, 'a_codec': 'aac',   'container': 'mp4'},
    '325': {'abr': 384, 'a_codec': 'dts',   'container': 'mp4'},
    '327': {'abr': 256, 'a_codec': 'aac',   'container': 'mp4'},
    '328': {'abr': 384, 'a_codec': 'eac3',  'container': 'mp4'},

    '338': {'abr': 480, 'a_codec': 'opus',  'container': 'webm'},
    '380': {'abr': 384, 'a_codec': 'ac3',   'container': 'mp4'},
    '599': {'abr': 30,  'a_codec': 'aac',   'container': 'mp4'},
    '600': {'abr': 35,  'a_codec': 'opus',  'container': 'webm'},
    '773': {'abr': 900, 'a_codec': 'iamf',  'container': 'mp4'},
    '774': {'abr': 256, 'a_codec': 'opus',  'container': 'webm'},
}


class Config:
    """
        Configuration class to store global settings and objects.
    """
    mode: str = "inplace"
    source_directory: Optional[os.PathLike[str]] = None
    target_directory: Optional[os.PathLike[str]] = None

    video_quality: int = 720
    video_fps: int = 30
    video_codec: str = "h264"
    audio_quality: int = 128
    no_match: str = "closest"

    recursive: bool = True
    threads: int = 3
    retries: int = 3

    video_format_priority: list = ["136"]
    audio_format_priority: list = ["140"]

    excel_path = None
    workbook = None
    sheet = None

    @classmethod
    def initialize(cls, args):
        """
        Initialize configuration parameters from command-line arguments.
        """
        cls.mode = args.mode
        cls.source_directory = args.source_dir
        cls.video_quality = args.video_quality
        cls.video_fps = args.video_fps
        cls.video_codec = args.video_codec
        cls.audio_quality = args.audio_quality
        cls.no_match = args.no_match
        cls.recursive = args.recursive
        cls.threads = args.threads
        cls.retries = args.retries

        if cls.mode == 'inplace':
            cls.target_directory = cls.source_directory
        else:
            if not args.target_dir:
                sys.exit("Error: --target_dir is required if --mode=by-channel.")
            cls.target_directory = args.target_dir


class VideoStatus(Enum):
    UNPROCESSED = ("Unprocessed", "FFFFFF")  # White
    DOWNLOADED = ("Downloaded", "00FF00")    # Green
    MOVED = ("Moved", "FFE600")              # Yellow
    SKIPPED = ("Skipped", "C0C0C0")          # Gray
    ERROR = ("Error", "FF0000")              # Red

    def __init__(self, label, color):
        self.label = label
        self._color = color

    def __str__(self):
        return self.label

    @property
    def color(self):
        return self._color


@dataclass
class Video:
    name: str
    old_path: os.PathLike[str]
    new_path: Optional[str] = None
    status: VideoStatus = VideoStatus.UNPROCESSED
    yt_id: str = 'N/A'
    yt_url: str = 'N/A'
    channel: str = 'N/A'
    duration: Optional[int] = None  # sec
    upload_date: Optional[date] = None
    log: List[str] = field(default_factory=list)
    format_ids: List[str] = field(default_factory=list)


global_logger = logging.getLogger("global_logger")
global_logger.setLevel(logging.INFO)
global_logger.addHandler(logging.StreamHandler())

def retry(func):
    """
        Retry decorator to attempt a function multiple times upon failure,
        uses the retry count from a global configuration.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        last_exception = None
        for attempt in range(Config.retries):
            try:
                result = func(*args, **kwargs)
                if result is not None:
                    return result
            except Exception as e:
                last_exception = e
                global_logger.warning(f"{func.__name__} failed on attempt {attempt + 1}: {e}")
        raise last_exception
    return wrapper


def parse_arguments():
    """
        Parse command-line arguments.
        :return: Parsed arguments object.
    """
    parser = argparse.ArgumentParser(
        description="Script to update video YouTube quality in local library and sort by channel folders."
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['inplace', 'by-channel'],
        default='inplace',
        help="Choose how to store the processed video: 'inplace' to replace files in the same folder,"
             " 'by-channel' to create channel subfolders (default: 'inplace')."
    )
    parser.add_argument(
        '--target_dir',
        type=str,
        default=None,
        help="Target directory where processed files will be saved if mode=by-channel. "
             "Not used if mode=inplace."
    )
    parser.add_argument(
        '--source_dir',
        type=str,
        default=os.getcwd(),
        help="Source directory containing videos to process (default: current working directory)."
    )
    parser.add_argument(
        '--video-quality',
        type=int,
        choices=HEIGHT,
        default=720,
        help="Required video quality (Default: 720)"
    )
    parser.add_argument(
        '--video-fps',
        type=int,
        choices=FPS,
        default=30,
        help="Required FPS (Default: 30)"
    )
    parser.add_argument(
        '--video-codec',
        type=str,
        choices=V_CODEC,
        default='h264',
        help="Required Video Codec (Default: 'h264')"
    )
    parser.add_argument(
        '--audio-quality',
        type=int,
        default=128,
        help="Preferred audio quality in Kbps (default: 128 )."
    )
    parser.add_argument(
        '--no_match',
        type=str,
        choices=['closest', 'skip', 'first_better', 'first_lower'],
        default='closest',
        help="Fallback behavior if no exact format is found:\n"
            " - closest: alternate closest higher/lower qualities\n"
            " - skip: use only the exact requested quality\n"
            " - first_better: prefer only higher qualities, ascending\n"
            " - first_lower: prefer only lower qualities, descending"
    )
    parser.add_argument(
        '--recursive',
        action='store_true',
        help="Enable recursive search in the source directory."
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=3,
        help="Number of concurrent threads (default: 3)."
    )
    parser.add_argument(
        '--retries',
        type=int,
        default=3,
        help="Number of retries for downloading files (default: 3)."
    )
    return parser.parse_args()


def ensure_latest_package(package_name):
    """
        Ensure that the specified package is updated to the latest version using pip.
        :param package_name: Name of the package to be updated.
        :raises SystemExit: If the pip update command fails.
    """
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", package_name])
        global_logger.info(f"{package_name} successfully updated to the latest version.")
    except subprocess.CalledProcessError as e:
        global_logger.warning(f"Failed to update {package_name}: {e}")
        sys.exit(1)


def build_video_format_priority_list():
    """
    Build a prioritized list of video format_ids based on quality, codec and fps,
    considering fallback behavior (Config.no_match).
    """
    target_height = Config.video_quality
    target_fps = Config.video_fps
    target_codec = Config.video_codec.lower()
    no_match = Config.no_match

    # Adjust quality list based on --no_match
    if no_match == 'skip':
        qualities = [target_height]
    elif no_match == 'first_better':
        qualities = [target_height] + [h for h in sorted(HEIGHT) if h > target_height]
    elif no_match == 'first_lower':
        qualities = [target_height] + [h for h in sorted(HEIGHT, reverse=True) if h < target_height]
    elif no_match == 'closest':
        higher = sorted([h for h in HEIGHT if h > target_height])
        lower = sorted([h for h in HEIGHT if h < target_height], reverse=True)
        qualities = [target_height]
        i = 0
        while i < max(len(higher), len(lower)):
            if i < len(higher):
                qualities.append(higher[i])
            if i < len(lower):
                qualities.append(lower[i])
            i += 1

    # Move user preference to front
    codecs = [target_codec] + [c.lower() for c in V_CODEC if c.lower() != target_codec]
    fps_list = [target_fps] + [f for f in FPS if f != target_fps]

    def index_or_max(value, lst):
        return lst.index(value) if value in lst else len(lst)

    def score(fmt: dict):
        q_score = index_or_max(fmt['height'], qualities)
        c_score = index_or_max(fmt['v_codec'].lower(), codecs)
        f_score = index_or_max(fmt['fps'], fps_list)
        return q_score * 100 + c_score * 10 + f_score

    sorted_formats = sorted(
        VIDEO_FORMATS.items(),
        key=lambda item: score(item[1])
    )

    # Save to config
    Config.video_format_priority = [fid for fid, _ in sorted_formats]


def build_audio_format_priority_list():
    """
    Build a list of audio format IDs sorted by the absolute difference between
    the format's abr and the desired value (lowest difference first).
    """
    target_abr = Config.audio_quality

    sorted_audio = sorted(
        AUDIO_FORMATS.items(),
        key=lambda item: abs(item[1]['abr'] - target_abr)
    )

    # Save to config
    Config.audio_format_priority = [fid for fid, _ in sorted_audio]


def initialize_excel():
    """
        Initialize an Excel workbook to log video processing statuses.
    """
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Processing Status"
    sheet.append(EXCEL_HEADERS)

    excel_path = os.path.join(Config.source_directory, "processing_status.xlsx")
    workbook.save(excel_path)

    Config.excel_path = excel_path
    Config.workbook = workbook
    Config.sheet = sheet


def write_log_and_status(video: Video):
    """
        Write a log and status entry into the Excel file.
        :param video: Video object with details about the video to download.
    """
    excel_path = Config.excel_path
    workbook = Config.workbook
    sheet = Config.sheet

    row = [
        video.name,
        video.channel,
        video.yt_url,
        video.status.label,
        "\n".join(video.log)
    ]
    sheet.append(row)
    status_cell = sheet.cell(row=sheet.max_row, column=4)
    status_cell.fill = PatternFill(start_color=video.status.color,
                                   end_color=video.status.color,
                                   fill_type="solid")
    workbook.save(excel_path)


def format_excel_sheet():
    """
    Adjust the Excel worksheet formatting by setting column widths, enabling text wrapping for logs,
    and ensuring text visibility without dynamic row height adjustment.
    """
    sheet = Config.sheet
    columns_to_adjust = {'A': 30, 'B': 25, 'D': 15}

    for col_letter, min_width in columns_to_adjust.items():
        max_length = 0
        for cell in sheet[col_letter]:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        sheet.column_dimensions[col_letter].width = max(max_length + 5, min_width)

    # Set wrap_text=True for all cells in column "Log"
    log_col_letter = 'E'
    for cell in sheet[log_col_letter]:
        cell.alignment = Alignment(wrap_text=True)
    sheet.column_dimensions[log_col_letter].width = 60

    # Set consistent row height
    for row in sheet.iter_rows():
        sheet.row_dimensions[row[0].row].height = 15
        for cell in row:
            if cell.column_letter != log_col_letter:
                cell.alignment = Alignment(horizontal='left', vertical='center', wrap_text=False)

    Config.workbook.save(Config.excel_path)


def is_video_file(file_path: str) -> bool:
    """
        Check if a file is a video file based on its extension.
        :param file_path: Path to the file.
        :return: True if the file is a video, False otherwise.
    """
    return os.path.isfile(file_path) and os.path.splitext(file_path)[1].lower() in VIDEO_EXTENSIONS


def get_video_files(source_dir: os.PathLike[str], recursive: bool = True) -> list[str]:
    """
        Get a list of video files from the source directory.
        :param source_dir: Directory containing video files.
        :param recursive: Whether to search recursively in subdirectories.
        :return: List of video file paths.
    """
    if recursive:
        video_files = [
            os.path.join(root, file)
            for root, _, files in os.walk(source_dir)
            for file in files if is_video_file(os.path.join(root, file))
        ]
    else:
        video_files = [
            os.path.join(source_dir, file)
            for file in os.listdir(source_dir)
            if is_video_file(os.path.join(source_dir, file))
        ]
    return video_files


def get_video_duration(video: Video) -> Optional[int]:
    """
        Retrieve the duration of a local video file.
        :param video: Video object containing the local path.
        :return: Duration of the video in seconds, or None if the duration cannot be retrieved.
    """
    try:
        with VideoFileClip(video.old_path) as clip:
            return int(clip.duration)
    except Exception as e:
        video.log.append(f"Unable to retrieve local video duration:{e}")
        return None


@retry
def search_youtube_video(video: Video) -> bool:
    """
        Search for a YouTube video by name and approximate duration match.
        :param video: Video object with local path and details.
        :return: True if matched, False otherwise.
    """
    search_query = f"ytsearch:{video.name}"
    video.log.append(f"Searching video...")

    with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
        results = ydl.extract_info(search_query, download=False).get('entries', [])
        if not results:
            video.log.append("No results found")
            return False

        local_duration = get_video_duration(video)
        if local_duration is None:
            video.log.append("Selecting the first result.")
            matched_result = results[0]
        else:
            matched_result = None
            for res in results[:10]:
                yt_duration = res.get('duration')
                if yt_duration and abs(local_duration - yt_duration) <= 3:
                    matched_result = res
                    break
            if matched_result:
                video.log.append("Found matching duration video.")
            else:
                video.log.append("No matching duration found. Selecting the first result as fallback.")
                matched_result = results[0]

        video.yt_id = matched_result.get('id', 'N/A')
        video.yt_url = matched_result.get('webpage_url', 'N/A')
        video.channel = matched_result.get('uploader', 'N/A')
        video.duration = int(matched_result.get('duration', None))
        upload_date = matched_result.get('upload_date', None)
        if upload_date:
            video.upload_date = datetime.strptime(upload_date, '%Y%m%d').date()
        video.format_ids = [f.get('format_id') for f in matched_result.get('formats', []) if 'format_id' in f]
        video.log.append(f"Selected video from channel '{video.channel}'")
        return True


def describe_format(format_id: str) -> str:
    """
       Return a human-readable description of a given YouTube format ID.
        :param format_id: The YouTube format ID to describe.
        :return: A string describing the format, such as '720p, 30 FPS' or '128 kbps'.
                Returns 'unknown format' if the ID is not recognized.
    """
    if format_id in VIDEO_FORMATS:
        f = VIDEO_FORMATS[format_id]
        return f"video {f.get('height', '?')}p, {f.get('fps', '?')} FPS"
    elif format_id in AUDIO_FORMATS:
        f = AUDIO_FORMATS[format_id]
        return f"audio {f.get('abr', '?')} kbps"
    return "unknown format"


@retry
def download_youtube_format(video: Video, output_dir: str, format_id: str) -> Optional[str]:
    """
        Download a specific YouTube video format.
        :param video: Video object with details about the video to download.
        :param output_dir: Directory where the video will be saved.
        :param format_id: YouTube format id to download.
        :return: Path to the downloaded file if successful, None otherwise.
    """
    try:
        video.log.append(f"Downloading format {format_id} ({describe_format(format_id)}) ...")
        ydl_opts = {
            'format': format_id,
            'outtmpl': os.path.join(output_dir, '%(title)s.%(ext)s'),
            'quiet': True
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(video.yt_url, download=True)
            file_path = ydl.prepare_filename(info_dict)
            video.log.append(f"Format {format_id} downloaded successfully.")
            return file_path
    except Exception as e:
        video.log.append(f"Error downloading format {format_id}: {e}")
        return None


@retry
def merge_video_audio(video: Video, video_path: str, audio_path: str, merged_path: str) -> Optional[str]:
    """
        Merge video and audio into a single file.
        :param video: Video object with details about the video to download.
        :param video_path: Path to the video file.
        :param audio_path: Path to the audio file.
        :param merged_path: Path to save the merged file.
        :return: Path to the merged file, or None if failed.
    """
    try:
        video.log.append("Starting merge of video and audio...")
        command = [
            "ffmpeg", "-i", video_path, "-i", audio_path,
            "-c:v", "libx264", "-c:a", "aac", merged_path,
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode != 0:
            video.log.append(f"Ffmpeg error: {result.stderr.decode()}")
            return None

        video.log.append(f"Merged video saved to target directory")
        return merged_path
    except Exception as e:
        video.log.append(f"Error during merging: {e}")
        return None


def download_and_merge_video_audio(video: Video, target_dir: str, v_fmt: str, a_fmt: str) -> Optional[str]:
    """
        Download and merge video and audio from a YouTube video into a single file.
        :param video: Video object with details about the video to download.
        :param target_dir: Path to the folder where the video should be saved.
        :param v_fmt: video format id.
        :param a_fmt: audio format id.
        :return: Path to the merged video file with 'tmp' extension, or None if the process fails.
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_video_path = download_youtube_format(video, temp_dir, v_fmt)
            temp_audio_path = download_youtube_format(video, temp_dir, a_fmt)
            merged_path = os.path.join(target_dir, f"[TEMP]{video.name}.mp4")

            merged_result = merge_video_audio(video, temp_video_path, temp_audio_path, merged_path)
            if not merged_result:
                video.log.append("Failed to merge video and audio.")
                return None

            video.log.append("Video/audio merged and saved as [TEMP] file successfully")
            return merged_result

    except Exception as e:
        video.log.append(f"Error in download and merge process: {e}")
        return None


def set_file_modification_date(video: Video, upload_date: Optional[datetime.date] = None):
    """
        Sets the file's modification date to the specified upload date obtained from YouTube.
        filepath (str): The path to the file whose modification date will be changed.
        upload_date (Optional[datetime.date]): The new modification date to set for the file.
    """
    if upload_date:
        try:
            new_datetime = datetime.combine(upload_date, datetime.min.time())
            timestamp = new_datetime.timestamp()
            os.utime(video.new_path, (timestamp, timestamp))
            video.log.append("The modification date is set successfully.")
        except Exception as e:
            video.log.append(f"The modification date is not set {e}.")


def video_already_exists(video: Video, target_folder: str) -> bool:
    """
        Check if a video with the given name already exists in the specified folder.
        :param video: Video object with details about the video to download.
        :param target_folder: Path to the folder where the video might exist.
        :return: True if the video exists, False otherwise.
    """
    if any(os.path.splitext(f)[0] == video.name for f in os.listdir(target_folder)):
        video.log.append(f"Video already exists in target folder. Skipping.")
        return True
    return False


@retry
def is_local_video_matching_config(video: Video) -> bool:
    """
    Returns True if the local video matches the configured quality (height)
    and FPS is within ±5 of the required FPS (if defined).
    """
    try:
        with VideoFileClip(video.old_path) as clip:
            local_height = clip.size[1]  # Video height
            local_fps = round(clip.fps)

        video.log.append(f"Local resolution is {local_height}p/{local_fps}fps.")

        if local_height != Config.video_quality or abs(local_fps - Config.video_fps) > 5:
            video.log.append(f"Local resolution does not match required.")
            return False

        else:
            video.log.append(f"Local resolution match required.")
            return True

    except Exception as e:
        video.log.append(f"Failed to check local video quality: {e}")
        return False


def process_video_task(file_path):
    """
       Process a single video file: check resolution, search for YouTube data, and perform appropriate actions.
       :param file_path: Path to the video file being processed.
    """
    video = Video(old_path=file_path,
                  name=os.path.splitext(os.path.basename(file_path))[0]
                  )
    video.log.append(f"Started processing video '{video.name}'")

    with open(os.devnull, 'w') as o_null, redirect_stdout(o_null), redirect_stderr(o_null):
        try:
            if not search_youtube_video(video):
                video.status = VideoStatus.ERROR
                return

            if Config.no_match == 'skip' and Config.video_format_priority[0] not in video.format_ids:
                video.log.append(
                    f"Exact required video format '{Config.video_format_priority[0]}' not found. "
                    f"No fallback allowed (no_match='skip'). Skipping download."
                )
                video.status = VideoStatus.SKIPPED
                return

            # Setup output paths
            if Config.mode == 'inplace':
                target_folder = os.path.dirname(video.old_path)
            else:
                target_folder = os.path.join(Config.target_directory, video.channel)
                os.makedirs(target_folder, exist_ok=True)
            video.new_path = os.path.join(target_folder, f"{video.name}.mp4")

            # Skip if file already exists in target
            if Config.mode == 'by-channel' and video_already_exists(video, target_folder):
                os.remove(video.old_path)
                video.log.append("Old file deleted successfully.")
                video.status = VideoStatus.SKIPPED
                return

            # Skip/move if local video is good enough
            if is_local_video_matching_config(video):
                if Config.mode == 'by-channel':
                    shutil.move(video.old_path, target_folder)
                    video.log.append("Local file matches required quality. Moved to channel folder.")
                    video.status = VideoStatus.MOVED
                else:
                    video.log.append("Local file matches required quality. Skipping download.")
                    video.status = VideoStatus.SKIPPED
                return

            # Select best available formats
            video_fmt_id = next((fid for fid in Config.video_format_priority if fid in video.format_ids), None)
            audio_fmt_id = next((fid for fid in Config.audio_format_priority if fid in video.format_ids), None)

            if not video_fmt_id or not audio_fmt_id:
                video.log.append("Required formats are not available. Using existing file.")
                if Config.mode == 'by-channel':
                    shutil.move(video.old_path, target_folder)
                    video.log.append("Existing file moved to channel folder.")
                    video.status = VideoStatus.MOVED
                else:
                    video.log.append("Mode=inplace; leaving file as is.")
                    video.status = VideoStatus.SKIPPED
                return

            # Download + merge
            video.log.append("Downloading and merging video-format and audio-format ...")
            merged_path = download_and_merge_video_audio(video, target_folder, video_fmt_id, audio_fmt_id)

            if merged_path:
                if Config.mode == 'inplace':
                    os.replace(merged_path, video.old_path)
                    video.log.append("File replaced 'in-place' successfully.")
                else:
                    os.rename(merged_path, video.new_path)
                    video.log.append("Merged [Temp] file renamed successfully.")
                    os.remove(video.old_path)
                    video.log.append("Old file deleted successfully.")
                video.status = VideoStatus.DOWNLOADED
            else:
                video.log.append("Download or merge failed. Video not processed.")
                video.status = VideoStatus.ERROR

        except Exception as e:
            video.log.append(f"Error processing video: {e}")
            video.status = VideoStatus.ERROR

        finally:
            if video.status != VideoStatus.ERROR:
                set_file_modification_date(video, video.upload_date)
            video.log.append("Processing completed.")
            write_log_and_status(video)


def process_videos():
    """
        Process all video files in the source directory, utilizing multithreading for efficiency.
    """

    video_files = get_video_files(Config.source_directory, Config.recursive)

    global_logger.info(f"Found {len(video_files)} videos for processing.")

    with ThreadPoolExecutor(Config.threads) as executor:
        futures = {}
        for file_path in video_files:
            video_name = os.path.splitext(os.path.basename(file_path))[0]
            global_logger.info(f"Processing video: {video_name}")
            futures[executor.submit(process_video_task, file_path)] = file_path


        for future in as_completed(futures):
            video_path = futures[future]
            video_name = os.path.splitext(os.path.basename(video_path))[0]
            try:
                future.result()
                global_logger.info(f"Processing completed for: {video_name}")
            except Exception as e:
                global_logger.warning(f"Error processing {video_name}: {e}")


if __name__ == '__main__':
    ensure_latest_package("yt-dlp")
    ensure_latest_package("ffmpeg")

    params = parse_arguments()
    Config.initialize(params)
    build_video_format_priority_list()
    build_audio_format_priority_list()

    initialize_excel()
    process_videos()
    format_excel_sheet()

    global_logger.info(f"\nProcessing completed. \nReport saved at {Config.excel_path}")
