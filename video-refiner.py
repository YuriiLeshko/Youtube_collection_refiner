import os
import sys
import tempfile
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
from typing import List, Optional
from functools import wraps
import logging
from logging.handlers import BufferingHandler
from contextlib import redirect_stdout, redirect_stderr
import argparse

import yt_dlp
import openpyxl
from openpyxl.styles import Alignment, PatternFill
from moviepy.video.io.VideoFileClip import VideoFileClip


VIDEO_EXTENSIONS = [".mp4", ".mkv", ".avi", ".mov", ".flv"]
EXCEL_HEADERS = ["Video Name", "Channel", "Link", "Status", "Log"]
STATUS_COLORS = {
    "Downloaded": "00FF00",  # Green
    "Moved": "FFFF00",       # Yellow
    "Skipped": "C0C0C0",     # Gray
    "Error": "FF0000"        # Red
}


class Config:
    """
        Configuration class to store global settings and objects.
    """
    recursive: bool = True
    threads: int = 3
    retries: int = 3

    excel_path = None
    workbook = None
    sheet = None


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
        'target_dir',
        type=str,
        help="Target directory where processed files will be saved."
    )
    parser.add_argument(
        '--source_dir',
        type=str,
        default=os.getcwd(),
        help="Source directory containing videos to process (default: current working directory)."
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


def initialize_excel(dir_path: str):
    """
        Initialize an Excel workbook to log video processing statuses.
        :param dir_path: Directory where the Excel file will be saved.
    """
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Processing Status"
    sheet.append(EXCEL_HEADERS)

    excel_path = os.path.join(dir_path, "processing_status.xlsx")
    workbook.save(excel_path)

    Config.excel_path = excel_path
    Config.workbook = workbook
    Config.sheet = sheet


def get_buffered_logs(logger_name: str) -> list:
    logger = logging.getLogger(logger_name)
    logs = []
    for handler in logger.handlers:
        if isinstance(handler, BufferingHandler):
            formatted_logs = [handler.format(record) for record in handler.buffer]
            logs.extend(formatted_logs)
    return logs


def write_log_and_status(video_name, channel_name, link, status):
    """
        Write a log and status entry into the Excel file.
        :param video_name: Name of the video.
        :param channel_name: Name of the channel.
        :param link: Link to the video.
        :param status: Processing status (Downloaded, Moved, Skipped, Error).
    """
    excel_path = Config.excel_path
    workbook = Config.workbook
    sheet = Config.sheet

    buffered_logs = get_buffered_logs(f"{video_name}")

    row = [
        video_name,
        channel_name,
        link,
        status,
        "\n".join(buffered_logs)
    ]
    sheet.append(row)
    status_cell = sheet.cell(row=sheet.max_row, column=4)
    status_cell.fill = PatternFill(start_color=STATUS_COLORS[status],
                                   end_color=STATUS_COLORS[status],
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
    return os.path.splitext(file_path)[1].lower() in VIDEO_EXTENSIONS


def get_video_files(source_dir: str, recursive: bool = True) -> list[str]:
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
            for file in files if is_video_file(file)
        ]
    else:
        video_files = [
            os.path.join(source_dir, f)
            for f in os.listdir(source_dir)
            if os.path.isfile(os.path.join(source_dir, f)) and is_video_file(f)
        ]
    return video_files


@retry
def search_youtube_video(video_name: str) -> Optional[dict]:
    """
       Search for a YouTube video by name.
       :param video_name: Name of the video to search for.
       :return: Dictionary containing video information, or None if not found.
    """
    local_logger = logging.getLogger(f"{video_name}")

    search_query = f"ytsearch:{video_name}"
    local_logger.info(f"Searching video...")
    with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
        results = ydl.extract_info(search_query, download=False).get('entries', [])
        if results:
            result = results[0]
            local_logger.info(f"Found video from channel: {result['uploader']}")
            return result
        else:
            local_logger.warning("No results found")
            raise Exception("No results found")


@retry
def download_youtube_video(video_name: str, video_url: str, output_dir: str, format_id: str) -> Optional[str]:
    local_logger = logging.getLogger(f"{video_name}")
    try:
        local_logger.info(f"Downloading format {format_id}...")
        ydl_opts = {
            'format': format_id,
            'outtmpl': os.path.join(output_dir, '%(title)s.%(ext)s'),
            'quiet': True
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(video_url, download=True)
            file_path = ydl.prepare_filename(info_dict)
            local_logger.info(f"Format {format_id} downloaded successfully.")
            return file_path
    except Exception as e:
        local_logger.warning(f"Error downloading format {format_id}: {e}")
        return None
@retry
def merge_video_audio(video_name, video_path: str, audio_path: str, merged_path: str) -> Optional[str]:
    """
        Merge video and audio into a single file.
        :param video_path: Path to the video file.
        :param audio_path: Path to the audio file.
        :param merged_path: Path to save the merged file.
        :return: Path to the merged file, or None if failed.
    """
    local_logger = logging.getLogger(f"{video_name}")
    try:
        local_logger.info("Starting merge of video and audio...")
        command = [
            "ffmpeg", "-i", video_path, "-i", audio_path,
            "-c:v", "copy", "-c:a", "aac", merged_path,
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode != 0:
            local_logger.warning(f"FFmpeg error: {result.stderr.decode()}")
            return None

        local_logger.info(f"Merged video saved to target directory")
        return merged_path
    except Exception as e:
        local_logger.warning(f"Error during merging: {e}")
        return None


def download_and_merge_video_audio(video_name, video_url, output_path, original_name) -> Optional[str]:
    """
        Download and merge video and audio from a YouTube video into a single file.
        :param video_url: URL of the YouTube video.
        :param output_path: Directory where the final merged file will be saved.
        :param original_name: Original name for the output file.
        :return: Path to the merged video file, or None if the process fails.
    """
    local_logger = logging.getLogger(f"{video_name}")
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_video_path = download_youtube_video(video_name, video_url, temp_dir, '136')
            temp_audio_path = download_youtube_video(video_name, video_url, temp_dir, '140')
            merged_path = os.path.join(output_path, f"{original_name}.mp4")

            merged_result = merge_video_audio(video_name, temp_video_path, temp_audio_path, merged_path)
            if not merged_result:
                local_logger.warning("Failed to merge video and audio.")
                return None

            local_logger.info("Video/audio merged and saved successfully")
            return merged_result

    except Exception as e:
        local_logger.warning(f"Error in download and merge process: {e}")
        return None


def video_already_exists(video_name: str, channel_folder: str) -> bool:
    """
        Check if a video with the given name already exists in the specified folder.
        :param video_name: Name of the video to check.
        :param channel_folder: Path to the folder where the video might exist.
        :return: True if the video exists, False otherwise.
    """
    local_logger = logging.getLogger(f"{video_name}")
    if any(os.path.splitext(f)[0] == video_name for f in os.listdir(channel_folder)):
        local_logger.info(f"Video already exists in target folder. Skipping.")
        return True
    return False


@retry
def handle_local_resolution(file_path: str) -> bool:
    """
        Check the resolution of a local video file.
        :param file_path: Path to the video file.
        :return: True if the resolution is 720p or higher, False otherwise.
    """
    file_name = os.path.basename(file_path)
    video_name = os.path.splitext(file_name)[0]
    local_logger = logging.getLogger(f"{video_name}")
    try:
        with VideoFileClip(file_path) as video:
            local_resolution = video.size[1]  # Video height
        local_logger.info(f"Local resolution is {local_resolution}.")
        if local_resolution and local_resolution >= 720:
            local_logger.info(f"Local resolution is >= 720p. Moving to target folder.")
            return True
        return False
    except Exception as e:
        local_logger.warning(f"Error processing video resolution: {e}")
        return False


def extract_required_formats(formats: List[dict]) -> dict:
    """
        Extract required formats (22, 136, 140) from the list of available formats.
        :param formats: List of available formats from the YouTube video info.
        :return: Dictionary with required format IDs as keys and format info as values.
    """
    required_formats = {
        "22": None,    # Video + Audio, 720p
        "136": None,   # Video only, 720p
        "140": None    # Audio only
    }
    for fmt in formats:
        if fmt['format_id'] in required_formats:
            required_formats[fmt['format_id']] = fmt

    return required_formats


def safe_move(source_path: str, channel_folder: str, file_name: str):
    """
        Safely move a file to the target directory, ensuring no overwrites.
        :param source_path: Source file path.
        :param channel_folder: Target folder for the file.
        :param file_name: Name of the file.
    """
    dest_path = os.path.join(channel_folder, file_name)
    counter = 1
    while os.path.exists(dest_path):
        base, ext = os.path.splitext(dest_path)
        dest_path = f"{base}_[{counter}]{ext}"
        counter += 1

    shutil.move(source_path, dest_path)


def process_video_task(file_path, target_dir):
    """
       Process a single video file: check resolution, search for YouTube data, and perform appropriate actions.
       :param file_path: Path to the video file being processed.
       :param target_dir: Directory where processed files will be stored.
    """
    file_name = os.path.basename(file_path)
    video_name = os.path.splitext(file_name)[0]
    global_logger.info(f"Processing video: {video_name}")

    # Logger search is based on his unique name that matches the name of the video
    local_logger = logging.getLogger(f"{video_name}")
    local_logger.setLevel(logging.INFO)
    buffer_handler = BufferingHandler(32)
    local_logger.addHandler(buffer_handler)

    with open(os.devnull, 'w') as o_null, redirect_stdout(o_null), redirect_stderr(o_null):
        try:
            result = search_youtube_video(video_name)
            if not result:
                write_log_and_status(video_name, "N/A", "N/A", "Error")
                return

            channel_name = result.get('uploader', 'Unknown Channel')
            youtube_video_url = result.get('webpage_url', 'Unknown URL')
            channel_folder = os.path.join(target_dir, channel_name)
            os.makedirs(channel_folder, exist_ok=True)

            if video_already_exists(video_name, channel_folder):
                os.remove(file_path)
                local_logger.info(f"Old file deleted successfully.")
                write_log_and_status(video_name, channel_name, youtube_video_url, "Skipped")
                return

            if handle_local_resolution(file_path):
                safe_move(file_path, channel_folder, file_name)
                write_log_and_status(video_name, channel_name, youtube_video_url, "Moved")
                return

            formats = extract_required_formats(result['formats'])
            local_logger.info(f"Available formats: {', '.join(formats)}")

            if formats["22"]:
                local_logger.info("Format 22 found. Downloading directly...")
                success = download_youtube_video(video_name, youtube_video_url, channel_folder, '22')
                if success:
                    os.remove(file_path)
                    local_logger.info(f"Old file deleted successfully.")
                    write_log_and_status(video_name, channel_name, youtube_video_url, "Downloaded")
                else:
                    write_log_and_status(video_name, channel_name, youtube_video_url, "Error")
                return

            if formats["136"] and formats["140"]:
                local_logger.info("Merging formats 136 and 140...")
                merged_path = download_and_merge_video_audio(video_name, youtube_video_url, channel_folder, video_name)
                if merged_path:
                    os.remove(file_path)
                    local_logger.info(f"Old file deleted successfully.")
                    write_log_and_status(video_name, channel_name, youtube_video_url, "Downloaded")
                else:
                    write_log_and_status(video_name, channel_name, youtube_video_url, "Error")
            else:
                local_logger.warning("Neither format 22 nor formats 136 and 140 are available. Moving existing file.")
                safe_move(file_path, channel_folder, file_name)
                local_logger.info(f"File moved to target folder.")
                write_log_and_status(video_name, channel_name, youtube_video_url, "Moved")

        except Exception as e:
            local_logger.warning(f"Error processing video {video_name}: {e}")
            write_log_and_status(video_name, "N/A", "N/A", "Error")


def process_videos(source_dir: str, target_dir: str, max_threads: int = 3):
    """
        Process all video files in the source directory, utilizing multithreading for efficiency.
        :param source_dir: Directory containing video files to process.
        :param target_dir: Directory where processed files will be stored.
        :param max_threads: Maximum number of threads for concurrent processing.
    """

    video_files = get_video_files(source_dir, Config.recursive)

    global_logger.info(f"Found {len(video_files)} videos for processing.")

    with ThreadPoolExecutor(max_threads) as executor:
        futures = {
            executor.submit(process_video_task, file_path, target_dir):
                file_path for file_path in video_files
        }

        for future in as_completed(futures):
            video_path = futures[future]
            try:
                future.result()
                global_logger.info(f"Processing completed for: {video_path}")
            except Exception as e:
                global_logger.warning(f"Error processing {video_path}: {e}")


if __name__ == '__main__':
    ensure_latest_package("yt-dlp")
    ensure_latest_package("ffmpeg")

    params = parse_arguments()

    source_directory = params.source_dir
    target_directory = params.target_dir

    Config.recursive = params.recursive
    Config.threads = params.threads
    Config.retries = params.retries

    initialize_excel(source_directory)
    process_videos(source_directory, target_directory, max_threads=Config.threads)

    format_excel_sheet()
    global_logger.info(f"\nProcessing completed. \nReport saved at {Config.excel_path}")
