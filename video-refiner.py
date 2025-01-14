import os
import tempfile
import shutil
import openpyxl
from openpyxl.styles import Alignment, PatternFill
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
from typing import List, Optional

from moviepy.video.io.VideoFileClip import VideoFileClip
import yt_dlp


VIDEO_EXTENSIONS = [".mp4", ".mkv", ".avi", ".mov", ".flv"]
EXCEL_HEADERS = ["Video Name", "Channel", "Link", "Status", "Log"]
STATUS_COLORS = {
    "Downloaded": "00FF00",  # Green
    "Moved": "FFFF00",       # Yellow
    "Skipped": "C0C0C0",     # Gray
    "Error": "FF0000"        # Red
}


def retry(max_retries=3):
    """
        Retry decorator to attempt a function multiple times upon failure.
        :param max_retries: Maximum number of retries allowed.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    if result is not None:
                        return result
                except Exception as e:
                    last_exception = e
                    print(f"{func.__name__} failed on attempt {attempt + 1}: {e}")
            raise last_exception
        return wrapper
    return decorator


def initialize_excel(dir_path: str) -> str:
    """
        Initialize an Excel workbook to log video processing statuses.
        :param dir_path: Directory where the Excel file will be saved.
        :return: Path to the created Excel file.
    """
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = "Processing Status"
    sheet.append(EXCEL_HEADERS)

    excel_path = os.path.join(dir_path, "processing_status.xlsx")
    workbook.save(excel_path)
    return excel_path


def write_log_and_status(excel_path, workbook, sheet, video_name, channel_name, link, status, log):
    """
        Write a log and status entry into the Excel file.
        :param excel_path: Path to the Excel file.
        :param workbook: OpenPyXL workbook object.
        :param sheet: OpenPyXL worksheet object.
        :param video_name: Name of the video.
        :param channel_name: Name of the channel.
        :param link: Link to the video.
        :param status: Processing status (Downloaded, Moved, Skipped, Error).
        :param log: List of log messages.
    """
    row = [
        video_name,
        channel_name,
        link,
        status,
        "\n".join(log)
    ]
    sheet.append(row)
    status_cell = sheet.cell(row=sheet.max_row, column=4)
    status_cell.fill = PatternFill(start_color=STATUS_COLORS[status],
                                   end_color=STATUS_COLORS[status],
                                   fill_type="solid")
    workbook.save(excel_path)


def adjust_column_width_and_row_height(sheet):
    """
        Adjust the column width and row height for better readability.
        :param sheet: OpenPyXL worksheet object.
    """
    columns_to_adjust = {'A': 30, 'B': 25, 'D': 15}

    for col_letter in columns_to_adjust.keys():
        max_length = 0
        for cell in sheet[col_letter]:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        sheet.column_dimensions[col_letter].width = max(max_length + 2, columns_to_adjust[col_letter])

    for row in sheet.iter_rows():
        for cell in row:
            cell.alignment = Alignment(horizontal='left', vertical='center', wrap_text=False)
        sheet.row_dimensions[row[0].row].height = 15


def is_video_file(file_path: str) -> bool:
    """
        Check if a file is a video file based on its extension.
        :param file_path: Path to the file.
        :return: True if the file is a video, False otherwise.
    """
    return os.path.splitext(file_path)[1].lower() in VIDEO_EXTENSIONS


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


@retry(max_retries=3)
def search_youtube_video(video_name: str, log: List[str]) -> Optional[dict]:
    """
       Search for a YouTube video by name.
       :param video_name: Name of the video to search for.
       :param log: List of log messages.
       :return: Dictionary containing video information, or None if not found.
    """
    search_query = f"ytsearch:{video_name}"
    log.append(f"Searching for: {search_query}")
    with yt_dlp.YoutubeDL({'quiet': True}) as ydl:
        results = ydl.extract_info(search_query, download=False).get('entries', [])
        if results:
            result = results[0]
            log.append(f"Found video: {result['webpage_url']} from channel: {result['uploader']}")
            return result
        else:
            log.append("No results found")
            raise Exception("No results found")


@retry(max_retries=3)
def download_youtube_video(video_url: str, output_dir: str, format_id: str, log: List[str]) -> Optional[str]:
    """
        Download a specific format of a YouTube video.
        :param video_url: URL of the video.
        :param output_dir: Directory where the video will be saved.
        :param format_id: Format ID to download.
        :param log: List of log messages.
        :return: Path to the downloaded file, or None if failed.
    """
    try:
        log.append(f"Downloading format {format_id}...")
        ydl_opts = {
            'format': format_id,
            'outtmpl': os.path.join(output_dir, '%(title)s.%(ext)s'),
            'quiet': True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(video_url, download=True)
            file_path = ydl.prepare_filename(info_dict)
            log.append(f"Format {format_id} downloaded successfully: {file_path}")
            return file_path
    except Exception as e:
        log.append(f"Error downloading format {format_id}: {e}")
        return None


@retry(max_retries=3)
def merge_video_audio(video_path: str, audio_path: str, merged_path: str, log: List[str]) -> Optional[str]:
    """
        Merge video and audio into a single file.
        :param video_path: Path to the video file.
        :param audio_path: Path to the audio file.
        :param merged_path: Path to save the merged file.
        :param log: List of log messages.
        :return: Path to the merged file, or None if failed.
    """
    try:
        log.append("Starting merge of video and audio...")
        command = [
            "ffmpeg", "-i", video_path, "-i", audio_path,
            "-c:v", "copy", "-c:a", "aac", merged_path
        ]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode != 0:
            log.append(f"FFmpeg error: {result.stderr.decode()}")
            return None

        log.append(f"Merged video saved to {merged_path}")
        return merged_path
    except Exception as e:
        log.append(f"Error during merging: {e}")
        return None


def download_and_merge_video_audio(video_url, output_path, original_name, log: List[str]) -> Optional[str]:
    """
        Download and merge video and audio from a YouTube video into a single file.
        :param video_url: URL of the YouTube video.
        :param output_path: Directory where the final merged file will be saved.
        :param original_name: Original name for the output file.
        :param log: List to store log messages for the process.
        :return: Path to the merged video file, or None if the process fails.
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_video_path = download_youtube_video(video_url, temp_dir, '136', log)
            temp_audio_path = download_youtube_video(video_url, temp_dir, '140', log)
            merged_path = os.path.join(output_path, f"{original_name}.mp4")

            merged_result = merge_video_audio(temp_video_path, temp_audio_path, merged_path, log)
            if not merged_result:
                log.append("Failed to merge video and audio.")
                return None

            log.append("Video/audio merged and saved successfully")
            return merged_result

    except Exception as e:
        log.append(f"Error in download and merge process: {e}")
        return None


def video_already_exists(video_name: str, channel_folder: str, log: List[str]) -> bool:
    """
        Check if a video with the given name already exists in the specified folder.
        :param video_name: Name of the video to check.
        :param channel_folder: Path to the folder where the video might exist.
        :param log: List to store log messages.
        :return: True if the video exists, False otherwise.
    """
    if any(os.path.splitext(f)[0] == video_name for f in os.listdir(channel_folder)):
        log.append(f"Video '{video_name}' already exists in target folder. Skipping.")
        return True
    return False


@retry(max_retries=3)
def handle_local_resolution(file_path: str, log: List[str]) -> bool:
    """
        Check the resolution of a local video file.
        :param file_path: Path to the video file.
        :param log: List to store log messages.
        :return: True if the resolution is 720p or higher, False otherwise.
    """
    try:
        with VideoFileClip(file_path) as video:
            local_resolution = video.size[1]  # Video height
        log.append(f"Local resolution is {local_resolution}.")
        if local_resolution and local_resolution >= 720:
            log.append(f"Local resolution is >= 720p. Moving to target folder.")
            return True
        return False
    except Exception as e:
        log.append(f"Error processing video resolution: {e}")
        return False


def extract_required_formats(formats: List[dict], log: List[str]) -> dict:
    """
        Extract required formats (22, 136, 140) from the list of available formats.
        :param formats: List of available formats from the YouTube video info.
        :param log: List to store log messages.
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
    log.append(f"Available formats: {', '.join([f for f in required_formats if required_formats[f]])}")
    return required_formats


def process_video_task(excel_path, workbook, sheet, file_path, target_dir):
    """
       Process a single video file: check resolution, search for YouTube data, and perform appropriate actions.
       :param excel_path: Path to the Excel file for logging.
       :param workbook: OpenPyXL workbook object.
       :param sheet: OpenPyXL worksheet object.
       :param file_path: Path to the video file being processed.
       :param target_dir: Directory where processed files will be stored.
    """
    file_name = os.path.basename(file_path)
    video_name = os.path.splitext(file_name)[0]
    log = [f"Processing video: {video_name}"]
    print(log[-1])

    try:
        result = search_youtube_video(video_name, log)
        if not result:
            write_log_and_status(excel_path, workbook, sheet,
                                 video_name, "N/A", "N/A", "Error", log)
            return

        channel_name = result.get('uploader', 'Unknown Channel')
        youtube_video_url = result.get('webpage_url', 'Unknown URL')
        channel_folder = os.path.join(target_dir, channel_name)
        os.makedirs(channel_folder, exist_ok=True)

        if video_already_exists(video_name, channel_folder, log):
            os.remove(file_path)
            log.append(f"Old file '{file_path}' deleted successfully.")
            write_log_and_status(excel_path, workbook, sheet,
                                 video_name, channel_name, youtube_video_url, "Skipped", log)
            return

        if handle_local_resolution(file_path, log):
            safe_move(file_path, channel_folder, file_name)
            write_log_and_status(excel_path, workbook, sheet,
                                 video_name, channel_name, youtube_video_url, "Moved", log)
            return

        formats = extract_required_formats(result['formats'], log)

        if formats["22"]:
            log.append("Format 22 found. Downloading directly...")
            success = download_youtube_video(youtube_video_url, channel_folder, '22', log)
            if success:
                os.remove(file_path)
                log.append(f"Old file '{file_path}' deleted successfully.")
                write_log_and_status(excel_path, workbook, sheet,
                                     video_name, channel_name, youtube_video_url, "Downloaded", log)
            else:
                write_log_and_status(excel_path, workbook, sheet,
                                     video_name, channel_name, youtube_video_url, "Error", log)
            return

        if formats["136"] and formats["140"]:
            log.append("Merging formats 136 and 140...")
            merged_path = download_and_merge_video_audio(youtube_video_url, channel_folder, video_name, log)
            if merged_path:
                os.remove(file_path)
                log.append(f"Old file '{file_path}' deleted successfully.")
                write_log_and_status(excel_path, workbook, sheet,
                                     video_name, channel_name, youtube_video_url, "Downloaded", log)
            else:
                write_log_and_status(excel_path, workbook, sheet,
                                     video_name, channel_name, youtube_video_url, "Error", log)
        else:
            log.append("Neither format 22 nor formats 136 and 140 are available. Moving existing file.")
            safe_move(file_path, channel_folder, file_name)
            log.append(f"File '{file_path}' moved to '{channel_folder}'.")
            write_log_and_status(excel_path, workbook, sheet,
                                 video_name, channel_name, youtube_video_url, "Moved", log)

    except Exception as e:
        log.append(f"Error processing video {video_name}: {e}")
        write_log_and_status(excel_path, workbook, sheet,
                             video_name, "N/A", "N/A", "Error", log)


def process_videos(source_dir: str, target_dir: str, max_threads: int = 3):
    """
        Process all video files in the source directory, utilizing multithreading for efficiency.
        :param source_dir: Directory containing video files to process.
        :param target_dir: Directory where processed files will be stored.
        :param max_threads: Maximum number of threads for concurrent processing.
    """
    excel_path = initialize_excel(source_dir)
    workbook = openpyxl.load_workbook(excel_path)
    sheet = workbook.active

    video_files = [
        os.path.join(root, file)
        for root, _, files in os.walk(source_dir)
        for file in files if is_video_file(file)
    ]

    print(f"Found {len(video_files)} videos for processing.")

    with ThreadPoolExecutor(max_threads) as executor:
        futures = {
            executor.submit(process_video_task, excel_path, workbook, sheet, file_path, target_dir):
                file_path for file_path in video_files
        }

        for future in as_completed(futures):
            video_path = futures[future]
            try:
                future.result()
                print(f"Processing completed for: {video_path}")
            except Exception as e:
                print(f"Error processing {video_path}: {e}")

    adjust_column_width_and_row_height(sheet)
    workbook.save(excel_path)
    print(f"\nProcessing completed. \nReport saved at {excel_path}")


if __name__ == '__main__':
    source_directory = '/run/media/ly/2A1F50327A5E76A2/Video/Alternative energy/Аксіальний генератор 1'
    target_directory = '/run/media/ly/2A1F50327A5E76A2/Video/Alternative energy/Sorted'
    process_videos(source_directory, target_directory)
